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
import datetime
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from twisted.trial import unittest

from exprec.server.cli import _experiment_start_end_to_json, \
    _metadata_to_json, \
    _records_to_dataframe
from exprec.server.exp_interface import BufferedExperimentInterface
from exprec.server.models import Base

dummy_data = {
    'experiment1': {
        'metadata': {'name': 'experiment_one'},
        'records' : {
            datetime.datetime.fromtimestamp(1): {'a': 1, 'b': 2, 'c': 3},
            datetime.datetime.fromtimestamp(2): {'a': 2, 'b': 3, 'c': 4},
            datetime.datetime.fromtimestamp(3): {'a': 3, 'b': 4, 'c': 5}
        },
    },
    'experiment2': {
        'metadata': {'name': 'experiment_two'},
        'records' : {
            datetime.datetime.fromtimestamp(1): {'a': 1, 'b': 2, 'c': 3},
            datetime.datetime.fromtimestamp(2): {'a': 2, 'b': 3, 'c': 4},
            datetime.datetime.fromtimestamp(3): {'a': 3, 'b': 4, 'c': 5},
            datetime.datetime.fromtimestamp(4): {'a': 4, 'b': 5, 'c': 6}
        },
    }
}


class TestCLI(unittest.TestCase):
    def setUp(self) -> None:
        self._buf_size = 10
        self._engine = create_engine(
            'sqlite:///:memory:',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool)
        Base.metadata.create_all(self._engine)
        self._session_fact = sessionmaker(bind=self._engine)
        self._session = self._session_fact()

        interface = BufferedExperimentInterface(chunk_size=self._buf_size,
                                                db_engine=self._engine)

        # fill with dummy data
        self.experiments = {}
        self._expected_rows = 0
        for exp_name, exp_data in dummy_data.items():
            exp_id = interface.new_experiment_instance()
            self.experiments[exp_name] = exp_id

            interface.add_metadata(exp_id, **exp_data['metadata'])
            for timestamp, variables in exp_data['records'].items():
                interface.record_variables(
                    experiment_id=exp_id,
                    timestamp=timestamp,
                    **variables
                )
                self._expected_rows += 1

            interface.finish_experiment_instance(exp_id)

        # flush, to make sure everything is in the database
        interface.flush()
        interface.close()

    def tearDown(self) -> None:
        self._session.close()

    def test_db_to_dataframe(self) -> None:
        df = _records_to_dataframe(self._session,
                                   exp_ids=self.experiments.values())
        self.assertEqual(df.shape[0], self._expected_rows)
        print('DataFrame:')
        print(df)

    def test_metadata_dict(self) -> None:
        metadata = _metadata_to_json(self._session,
                                     exp_ids=self.experiments.values())
        print('Metadata:')
        print(metadata)

        for exp_name, exp_id in self.experiments.items():
            expected_metadata = dummy_data[exp_name]['metadata']
            mdata = metadata[str(exp_id)]

            for k, v in expected_metadata.items():
                self.assertIn(k, mdata)
                self.assertEqual(mdata[k], v)

    def test_timing_dict(self) -> None:
        times = _experiment_start_end_to_json(self._session,
                                              self.experiments.values())
        print('Times:')
        print(times)

        for exp_id, time_dict in times.items():
            exp_id = uuid.UUID(exp_id)
            start = datetime.datetime.fromisoformat(time_dict['start'])
            end = datetime.datetime.fromisoformat(time_dict['end'])
