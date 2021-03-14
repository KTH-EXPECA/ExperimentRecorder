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
from twisted.trial import unittest

from exprec.exp_interface import BufferedExperimentInterface
from exprec.models import *


class TestBufferedDBAccess(unittest.TestCase):
    def setUp(self) -> None:
        self._buf_size = 10
        self._interface = BufferedExperimentInterface(buf_size=self._buf_size)
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
        self.assertEqual(len(all_metadata), 2)

        for mdata, (k, v) in zip(all_metadata, metadata_pairs.items()):
            self.assertEqual(mdata.label, k)
            self.assertEqual(mdata.value, v)

    def check_deferred_var_record(self) -> None:
        # variable recordings should be buffered
        assert self._buf_size > 1

        exp_id = self.test_make_experiment()

        # single update should not be flushed
        self._interface.record_variable(
            experiment_id=exp_id,
            name='variable',
            value=0,
            timestamp=datetime.datetime.now()
        )

        # check that variable is indeed not in database
        records = self._session\
            .query(VariableRecord) \
            .all()

        self.assertIsNone(records)

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
            self._interface.record_variable(
                experiment_id=exp_id,
                name='variable',
                value=i,
                timestamp=datetime.datetime.now()
            )
        self._interface.wait_for_flush()

        records = self._session \
            .query(VariableRecord) \
            .all()

        self.assertIsNotNone(records)
        self.assertEqual(len(records), self._buf_size + 1)



