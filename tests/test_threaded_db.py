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
import time

from sqlalchemy import create_engine
from twisted.trial import unittest

from exprec.db_session import ThreadedSessionWrapper
from exprec.models import *


class TestThreadedDBAccess(unittest.TestCase):
    def setUp(self) -> None:
        # set up an in-memory database connection
        engine = create_engine('sqlite:///test.db', echo=True)
        Base.metadata.create_all(engine)

        # self._session_maker = sessionmaker(bind=engine)
        self._buf_size = 10

        self._t_session = ThreadedSessionWrapper(engine,
                                                 buf_size=self._buf_size)

    def tearDown(self) -> None:
        self._t_session.shutdown()

    def test_blocking_commit_add(self):
        # basic test, test blocking operations
        exp_inst = ExperimentInstance()
        self._t_session.add_commit_immediately(exp_inst)

        metadata = ExperimentMetadata(instance_id=exp_inst.id,
                                      label='Test',
                                      value='Valid')
        self._t_session.add_commit_immediately(metadata)

    def test_deferred_commit_add(self):
        # test deferred commits
        exp_inst = ExperimentInstance()
        self._t_session.add_commit_deferred(exp_inst)

        self.assertIsNone(exp_inst.id)
        self._t_session.flush()
        time.sleep(0.1)  # wait for flush to finish
        self.assertIsNotNone(exp_inst.id)
