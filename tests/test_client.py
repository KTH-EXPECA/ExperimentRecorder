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
from twisted.test.proto_helpers import StringTransport
from twisted.trial import unittest

from exprec.client.client import *


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ExpRecClient()
        self.packer = MessagePacker()
        self.unpacker = MessageUnpacker()

        # when just initialized, all writing methods should raise
        self.assertRaises(UninitializedExperiment,
                          self.client.get_experiment_id)
        self.assertRaises(UninitializedExperiment,
                          self.client.write_metadata)
        self.assertRaises(UninitializedExperiment,
                          self.client.record_variables)

    def test_connection_made(self) -> None:
        self.client.makeConnection(StringTransport())

    def test_init_experiment(self) -> None:
        self.test_connection_made()
        expected_id = uuid.uuid4()

        # send an initialization message like the protocol would
        msg = make_message('welcome', {'instance_id': expected_id})
        self.client.dataReceived(self.packer.pack(msg))

        # should now be bound and not raise anything
        self.assertEqual(expected_id, self.client.get_experiment_id())

    def test_version(self) -> None:
        self.test_connection_made()

        # try sending a compatible version message to the client
        # should pass silently
        msg = make_message('version', {'major': self.client.version_major,
                                       'minor': self.client.version_minor})
        self.client.dataReceived(self.packer.pack(msg))

        # incompatible version makes the client disconnect forcefully and raise
        msg = make_message('version', {'major': self.client.version_major + 1,
                                       'minor': self.client.version_minor})
        with self.assertRaises(IncompatibleVersionException):
            self.client.dataReceived(self.packer.pack(msg))

    def test_record_variables(self) -> None:
        # must always init first
        self.test_init_experiment()

        # client should not be waiting for anything atm
        self.assertEqual(0, self.client._waiting_for_status)

        # send some variable records
        records = dict(a=1, b=3, c=1, d=2)
        self.client.record_variables(**records)

        # we should receive on the other end as a valid record message
        self.unpacker.feed(self.client.transport.value())
        self.client.transport.clear()

        msg_type, msg_payload = validate_message(next(self.unpacker))
        self.assertEqual(msg_type, 'record')
        self.assertIn('timestamp', msg_payload)
        self.assertIn('variables', msg_payload)
        self.assertDictEqual(msg_payload['variables'], records)

        # client should now be waiting for a reply
        self.assertEqual(1, self.client._waiting_for_status)

    def test_waiting_for_reply(self) -> None:
        # must always init first
        self.test_init_experiment()

        # make the client wait for a reply
        orig_waiting = self.client._waiting_for_status
        self.client._waiting_for_status += 1

        # send a status reply
        msg = make_message('status', {'success': True})
        self.client.dataReceived(self.packer.pack(msg))

        # check that waiting counter has been reduced
        self.assertEqual(orig_waiting, self.client._waiting_for_status)

    def test_record_metadata(self) -> None:
        # must always init first
        self.test_init_experiment()

        # client should not be waiting for anything atm
        self.assertEqual(0, self.client._waiting_for_status)

        # send some metadata
        metadata = dict(name='foobar', ac='ab')
        self.client.write_metadata(**metadata)

        # we should receive on the other end as a valid record message
        self.unpacker.feed(self.client.transport.value())
        self.client.transport.clear()

        msg_type, msg_payload = validate_message(next(self.unpacker))
        self.assertEqual(msg_type, 'metadata')
        self.assertDictEqual(msg_payload, metadata)

        # client should now be waiting for a reply
        self.assertEqual(1, self.client._waiting_for_status)

    def test_double_init(self) -> None:
        # can't initialize experiment twice
        self.test_init_experiment()
        self.assertRaises(ProtocolException, self.test_init_experiment)
