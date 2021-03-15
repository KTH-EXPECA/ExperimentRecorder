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
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from twisted.trial import unittest

from exprec.exp_interface import BufferedExperimentInterface
from exprec.models import *

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
            buf_size=self._buf_size,
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
        self.assertEqual(len(self._interface._buf), 0)

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
