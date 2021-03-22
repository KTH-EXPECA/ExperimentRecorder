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
from threading import Event

from twisted.test.proto_helpers import StringTransport
from twisted.trial import unittest

from exprec.client.client import *


class TestClient(unittest.TestCase):
    def setUp(self) -> None:
        self._exp_id_called = False

        def _exp_id_callback(exp_id: uuid.UUID):
            self.assertIsInstance(exp_id, uuid.UUID)
            self._exp_id_called = True

        d = Deferred().addCallback(_exp_id_callback)
        self.client = ExperimentClient(exp_id_deferred=d)
        self.packer = MessagePacker()
        self.unpacker = MessageUnpacker()

        # make the connection and check callbacks
        self.client.makeConnection(StringTransport())

        # send handshake messages like the server would
        # first, check that version sent by client is ok
        self.unpacker.feed(self.client.transport.value())
        self.client.transport.clear()

        msg = validate_message(next(self.unpacker))
        self.assertEqual(msg.mtype, 'version')
        self.assertEqual(msg.payload.get('major', -1),
                         self.client.version_major)
        self.assertEqual(msg.payload.get('minor', -1),
                         self.client.version_minor)

        # then, send a valid experiment id
        self.exp_id = uuid.uuid4()
        msg = make_message('welcome', {'instance_id': self.exp_id})
        self.client.dataReceived(self.packer.pack(msg))

        # check that the callback was invoked
        self.assertTrue(self._exp_id_called)

    def test_record_variables(self) -> None:
        # client should not be waiting for anything atm
        self.assertEqual(0, self.client.backlog)

        # send some variable records
        records = dict(a=1, b=3, c=1, d=2)
        timestamp = datetime.datetime.now()
        d = self.client.record_variables(timestamp=timestamp, **records)

        success_received = Event()  # simply to encapsulate a bool
        success_received.clear()

        # add a callback to the success
        def success_callback(_):
            success_received.set()

        d.addCallback(success_callback)

        # we should receive on the other end as a valid record message
        self.unpacker.feed(self.client.transport.value())
        self.client.transport.clear()

        msg = validate_message(next(self.unpacker))
        self.assertEqual(msg.mtype, 'record')
        self.assertIn('timestamp', msg.payload)
        self.assertIn('variables', msg.payload)
        self.assertDictEqual(msg.payload['variables'], records)

        # client should now be waiting for a reply
        self.assertEqual(1, self.client.backlog)

        # send a reply and verify the callback is executed
        reply = make_message('status', {'success': True})
        self.client.dataReceived(self.packer.pack(reply))

        self.assertTrue(success_received.is_set())
        # client should not be waiting for anything
        self.assertEqual(0, self.client.backlog)

    def test_record_metadata(self) -> None:
        # client should not be waiting for anything atm
        self.assertEqual(0, self.client.backlog)

        # send some metadata
        metadata = dict(name='foobar', ac='ab')
        d = self.client.write_metadata(**metadata)

        success_received = Event()  # simply to encapsulate a bool
        success_received.clear()

        # add a callback to the success
        def success_callback(_):
            success_received.set()

        d.addCallback(success_callback)

        # we should receive on the other end as a valid metadata message
        self.unpacker.feed(self.client.transport.value())
        self.client.transport.clear()

        msg = validate_message(next(self.unpacker))
        self.assertEqual(msg.mtype, 'metadata')
        self.assertDictEqual(msg.payload, metadata)

        # client should now be waiting for a reply
        self.assertEqual(1, self.client.backlog)

        # send a reply and verify the callback is executed
        reply = make_message('status', {'success': True})
        self.client.dataReceived(self.packer.pack(reply))

        self.assertTrue(success_received.is_set())
        # client should not be waiting for anything
        self.assertEqual(0, self.client.backlog)

    def test_finish(self) -> None:
        # client should not be waiting for anything atm
        self.assertEqual(0, self.client.backlog)

        # send finish message
        self.client.finish()
        self.unpacker.feed(self.client.transport.value())
        self.client.transport.clear()

        msg = validate_message(next(self.unpacker))

        self.assertEqual(msg.mtype, 'finish')
        self.assertIsNone(msg.payload)

