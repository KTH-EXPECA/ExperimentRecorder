#  Copyright (c) 2021 KTH Royal Institute of Technology
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from collections import deque

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from twisted.trial import unittest

from exprec.server.exp_interface import BufferedExperimentInterface
from exprec.server.models import *

default_metadata = {
    'default_1': 'foo',
    'default_2': 'bar'
}


class TestBufferedDBAccess(unittest.TestCase):
    def setUp(self) -> None:
        self._buf_size = 10
        self._engine = create_engine(
            'sqlite:///:memory:',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool)

        self._interface = BufferedExperimentInterface(
            chunk_size=self._buf_size,
            db_engine=self._engine,
            default_metadata=default_metadata
        )
        self._session = self._interface.session

    def tearDown(self) -> None:
        self._interface.close()

    def test_make_experiment(self) -> uuid.UUID:
        exp_id = self._interface.new_experiment_instance()
        self.assertIsInstance(exp_id, uuid.UUID)

        # check that the ID effectively exists in the database
        exp = self._session \
            .query(ExperimentInstance) \
            .filter_by(id=exp_id) \
            .first()

        self.assertIsNotNone(exp)

        # check that default metadata was added
        for k, v in default_metadata.items():
            result = self._session \
                .query(ExperimentMetadata) \
                .filter(ExperimentMetadata.instance_id == exp_id) \
                .filter(ExperimentMetadata.label == k) \
                .filter(ExperimentMetadata.value == v) \
                .first()

            self.assertIsNotNone(result)

        return exp_id

    def test_add_metadata(self) -> None:
        metadata_pairs = {
            'test1': 'metadata1',
            'test2': 'metadata2'
        }

        exp_id = self.test_make_experiment()
        self._interface.add_metadata(exp_id, **metadata_pairs)

        # check that the metadata has been added
        all_metadata = self._session \
            .query(ExperimentMetadata) \
            .order_by(ExperimentMetadata.label) \
            .all()
        self.assertEqual(len(all_metadata), 2 + len(default_metadata))

        for k, v in metadata_pairs.items():
            result = self._session \
                .query(ExperimentMetadata) \
                .filter(ExperimentMetadata.instance_id == exp_id) \
                .filter(ExperimentMetadata.label == k) \
                .filter(ExperimentMetadata.value == v) \
                .first()

            self.assertIsNotNone(result)

    def test_deferred_var_record(self) -> None:
        # variable recordings should be buffered
        assert self._buf_size > 1

        exp_id = self.test_make_experiment()

        # single update should not be flushed
        self._interface.record_variables(
            experiment_id=exp_id,
            timestamp=datetime.datetime.now(),
            variable=1
        )

        # check that variable is indeed not in database
        records = self._session \
            .query(VariableRecord) \
            .all()

        self.assertEqual(len(records), 0)

        # flush
        self._interface.flush(blocking=True)
        self.assertEqual(len(self._interface._chunk), 0)

        # now it is in the database
        records = self._session \
            .query(VariableRecord) \
            .all()

        self.assertIsNotNone(records)
        self.assertEqual(len(records), 1)

        # check automatic flushing
        for i in range(self._buf_size):
            self._interface.record_variables(
                experiment_id=exp_id,
                timestamp=datetime.datetime.now(),
                variable=i,
            )
        self._interface.wait_for_flush()

        records = self._session \
            .query(VariableRecord) \
            .all()

        self.assertIsNotNone(records)
        self.assertEqual(len(records), self._buf_size + 1)

    def test_finish_experiment(self) -> None:
        # at disconnection time, the protocol should timestamp the experiment
        exp_id = self._interface.new_experiment_instance()
        self._interface.finish_experiment_instance(exp_id)

        exp = self._session.query(ExperimentInstance) \
            .filter(ExperimentInstance.id == exp_id) \
            .first()

        self.assertIsNotNone(exp.end)
        self.assertIsInstance(exp.end, datetime.datetime)

    def test_get_records_as_dataframe(self) -> None:
        n_records = 1000

        a_df = pd.DataFrame(
            data={'a': np.linspace(-10, 10, num=n_records)},
            index=[datetime.datetime.fromtimestamp(i)
                   for i in np.linspace(1, 1e9, endpoint=False,
                                        num=n_records)]
        )

        b_df = pd.DataFrame(
            data={'b': np.logspace(-3, 2, num=n_records // 2)},
            index=[datetime.datetime.fromtimestamp(i)
                   for i in np.linspace(1, 1e9, endpoint=False,
                                        num=n_records // 2)]
        )

        data_df = a_df.join(other=b_df, how='outer', sort=True)

        exp_id = self._interface.new_experiment_instance()
        for t, (a, b) in data_df.iterrows():
            if np.isnan(b):
                self._interface.record_variables(
                    experiment_id=exp_id,
                    timestamp=t,
                    a=a
                )
            else:
                self._interface.record_variables(
                    experiment_id=exp_id,
                    timestamp=t,
                    a=a, b=b
                )

        # recorded, now get it back
        exp_df = self._interface.records_as_dataframe()

        # we use allclose and astype('float') since the database returns the
        # records as Decimal objects, messing a bit with our precision.
        self.assertTrue(np.allclose(exp_df['a'].astype('float').to_numpy(),
                                    data_df['a'].to_numpy()))

        # need to dropna since np.nan != np.nan...
        self.assertTrue(
            np.allclose(exp_df['b'].dropna().astype('float').to_numpy(),
                        data_df['b'].dropna().to_numpy())
        )

    def test_metadata_as_dict(self) -> None:
        exp_id = self._interface.new_experiment_instance()

        # the new instance should have the default metadata, now we add a
        # couple more elements
        metadata = {f'metadata_{i}': str(i * 2) for i in range(1000)}
        self._interface.add_metadata(experiment_id=exp_id, **metadata)

        # get the dict from the interface, should have structure
        # {id: {metadata1: foo, metadata2: bar, ...}}
        mdata_dict = self._interface.metadata_as_dict()
        self.assertIn(str(exp_id), mdata_dict)

        exp_metadata = mdata_dict[str(exp_id)]
        for k, v in default_metadata.items():
            self.assertIn(k, exp_metadata)
            self.assertEqual(v, exp_metadata[k])

        for k, v in metadata.items():
            self.assertIn(k, exp_metadata)
            self.assertEqual(v, exp_metadata[k])

    def test_start_end_as_dict(self) -> None:
        # init and finish a few experiments
        has_end = deque()
        for i in range(100):
            exp = self._interface.new_experiment_instance()
            self._interface.finish_experiment_instance(exp)
            has_end.append(str(exp))

        # make some without and end timestamp
        no_end = deque()
        for i in range(100):
            exp = self._interface.new_experiment_instance()
            no_end.append(str(exp))

        # get the dictionary
        start_end = self._interface.experiment_times_as_dict()

        for exp in has_end:
            # these ones should all have an end timestamp
            self.assertIn(exp, start_end)
            self.assertIn('start', start_end[exp])
            self.assertIn('end', start_end[exp])

            start = start_end[exp]['start']
            end = start_end[exp]['end']

            self.assertIsInstance(start, str)
            self.assertIsInstance(end, str)

            try:
                datetime.datetime.fromisoformat(start)
            except ValueError:
                self.fail(f'Could not parse date from start string: {start}')

            try:
                datetime.datetime.fromisoformat(end)
            except ValueError:
                self.fail(f'Could not parse date from end string: {end}')

        for exp in no_end:
            # these ones should not have an end timestamp
            self.assertIn(exp, start_end)
            self.assertIn('start', start_end[exp])
            self.assertIn('end', start_end[exp])

            start = start_end[exp]['start']
            end = start_end[exp]['end']

            self.assertIsInstance(start, str)
            self.assertIsNone(end)

            try:
                datetime.datetime.fromisoformat(start)
            except ValueError:
                self.fail(f'Could not parse date from start string: {start}')
