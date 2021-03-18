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

import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from twisted.internet.address import IPv4Address
from twisted.test.proto_helpers import StringTransport
from twisted.trial import unittest

from exprec.client.client import ExpRecClient
from exprec.server.exp_interface import BufferedExperimentInterface
from exprec.server.models import ExperimentInstance, ExperimentMetadata, \
    InstanceVariable, \
    VariableRecord
from exprec.server.protocol import MessageProtoFactory, MessageProtocol

test_metadata = {
    'name'       : 'test_client_and_server',
    'foo'        : 'bar',
    'lorem_ipsum': 'dolor_sit_amet'
}

var_names = [f'var_{uuid.uuid4().int:032x}' for i in range(30)]
test_records = []
for i in range(1000):
    record = {
        k: v for k, v in zip(var_names, np.random.uniform(size=len(var_names)))
    }
    test_records.append(record)


# noinspection DuplicatedCode
class TestClientServer(unittest.TestCase):
    # full integration test of the client + server
    def setUp(self) -> None:
        self.client = ExpRecClient()

        addr = IPv4Address(type='TCP', host='localhost', port=1312)
        self._engine = create_engine(
            'sqlite:///:memory:',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool)

        self._interface = BufferedExperimentInterface(db_engine=self._engine)
        self.session = self._interface.session
        factory = MessageProtoFactory(self._interface)

        self.server: MessageProtocol = factory.buildProtocol(addr)
        self.transport = StringTransport()

        # connect to transport
        self.client.makeConnection(self.transport)
        self.server.makeConnection(self.transport)

        # server should've sent welcome message
        self.client.dataReceived(self.transport.value())
        self.transport.clear()

        self.experiment_id = self.client.get_experiment_id()

        # check that experiment has a start timestamp
        start_timestamp, = self.session \
            .query(ExperimentInstance.start) \
            .filter(ExperimentInstance.id == self.experiment_id) \
            .first()
        self.assertIsNotNone(start_timestamp)
        self.assertIsInstance(start_timestamp, datetime.datetime)

        # check that experiment doesn't have an end timestamp yet
        end_timestamp, = self.session \
            .query(ExperimentInstance.end) \
            .filter(ExperimentInstance.id == self.experiment_id) \
            .first()
        self.assertIsNone(end_timestamp)

    def tearDown(self) -> None:
        self.transport.loseConnection()
        self.client.connectionLost()
        self.server.connectionLost()

        # check that experiment has an end timestamp now
        end_timestamp, = self.session \
            .query(ExperimentInstance.end) \
            .filter(ExperimentInstance.id == self.experiment_id) \
            .first()
        self.assertIsNotNone(end_timestamp)
        self.assertIsInstance(end_timestamp, datetime.datetime)

        self._interface.close()

    def test_send_metadata(self):
        # send metadata through the client and make sure it's received and
        # stored correctly

        self.assertEqual(self.client._waiting_for_status, 0)
        self.client.write_metadata(**test_metadata)
        self.assertEqual(self.client._waiting_for_status, 1)

        # pass message to server
        data = self.transport.value()
        self.transport.clear()
        self.server.dataReceived(data)

        # pass reply to client
        reply = self.transport.value()
        self.transport.clear()
        self.client.dataReceived(reply)
        self.assertEqual(self.client._waiting_for_status, 0)

        # check that metadata was correctly stored in the database
        for k, v in test_metadata.items():
            db_metadata = self.session \
                .query(ExperimentMetadata) \
                .filter(ExperimentMetadata.instance_id == self.experiment_id) \
                .filter(ExperimentMetadata.label == k) \
                .first()

            self.assertIsNotNone(db_metadata)
            self.assertEqual(db_metadata.value, v)

    def test_send_records(self):
        # send records through the client and make sure they're received and
        # stored correctly

        for record in test_records:
            self.assertEqual(self.client._waiting_for_status, 0)
            self.client.record_variables(**record)
            self.assertEqual(self.client._waiting_for_status, 1)

            # pass message to server
            data = self.transport.value()
            self.transport.clear()
            self.server.dataReceived(data)

            # pass reply to client
            reply = self.transport.value()
            self.transport.clear()
            self.client.dataReceived(reply)
            self.assertEqual(self.client._waiting_for_status, 0)

        # check that records were correctly stored in the database,
        # NEED TO ENSURE DATA HAS BEEN FLUSHED FIRST!
        self._interface.flush(blocking=True)
        for var in var_names:
            db_variables = self.session \
                .query(InstanceVariable) \
                .filter(InstanceVariable.instance_id == self.experiment_id) \
                .filter(InstanceVariable.name == var) \
                .all()

            self.assertEqual(len(db_variables), 1)
            db_variable = db_variables[0]

            db_records = self.session \
                .query(VariableRecord) \
                .filter(VariableRecord.variable_id == db_variable.id) \
                .all()

            self.assertEqual(len(db_records), len(test_records))
