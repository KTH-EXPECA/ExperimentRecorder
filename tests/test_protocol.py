# #  Copyright (c) 2021 KTH Royal Institute of Technology
# #
# #  Licensed under the Apache License, Version 2.0 (the "License");
# #  you may not use this file except in compliance with the License.
# #  You may obtain a copy of the License at
# #
# #         http://www.apache.org/licenses/LICENSE-2.0
# #
# #  Unless required by applicable law or agreed to in writing, software
# #  distributed under the License is distributed on an "AS IS" BASIS,
# #  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# #  See the License for the specific language governing permissions and
# #  limitations under the License.
from __future__ import annotations

import datetime
import time
import uuid

from twisted.test import proto_helpers
from twisted.trial import unittest

from exprec.exp_interface import BufferedExperimentInterface
from exprec.messages import make_message, validate_message
from exprec.protocol import MessagePacker, MessageProtoFactory, \
    MessageProtocol, \
    MessageUnpacker


class TestMessagePacking(unittest.TestCase):
    def setUp(self) -> None:
        self._packer = MessagePacker()
        self._unpacker = MessageUnpacker()

    def test_pack_unpack(self):
        message = {
            'date': datetime.datetime.fromtimestamp(time.monotonic()),
            'uuid': uuid.uuid4()
        }

        packed = self._packer.pack(message)
        self.assertIsNotNone(packed)

        self._unpacker.feed(packed)
        unpacked = next(self._unpacker)

        self.assertDictEqual(message, unpacked)


class TestProtocol(unittest.TestCase):
    def setUp(self) -> None:
        addr = ('dummy', 0)
        self._interface = BufferedExperimentInterface()
        factory = MessageProtoFactory(self._interface)

        self.proto: MessageProtocol = factory.buildProtocol(addr)
        self.transport = proto_helpers.StringTransport()
        self.proto.makeConnection(self.transport)

        # use the special packers
        self.packer = MessagePacker()
        self.unpacker = MessageUnpacker()

        # server should send two things, version and experiment instance
        self.unpacker.feed(self.transport.value())
        self.transport.clear()

        version_msg = next(self.unpacker)
        msg_type, msg = validate_message(version_msg)
        self.assertEqual(msg_type, 'version')
        self.assertEqual(msg['major'], self.proto.version_major)
        self.assertEqual(msg['minor'], self.proto.version_minor)

        welcome_msg = next(self.unpacker)
        msg_type, msg = validate_message(welcome_msg)
        self.assertEqual(msg_type, 'welcome')
        self.assertIn('instance_id', msg)

    def tearDown(self) -> None:
        self._interface.close()

    def test_send_metadata(self):
        metadata_msg = make_message('metadata', {'foo': 'bar'})
        self.proto.dataReceived(self.packer.pack(metadata_msg))

        # protocol should simply reply with a success message
        self.unpacker.feed(self.transport.value())
        self.transport.clear()

        reply = next(self.unpacker)
        rtype, rmsg = validate_message(reply)
        self.assertEqual(rtype, 'status')
        self.assertTrue(rmsg['success'])

    def test_send_record(self):
        record_msg = make_message(
            msg_type='record',
            payload={
                'timestamp': datetime.datetime.now(),
                'variables': {
                    'foo' : 666,
                    'acab': 1312
                }
            }
        )

        # protocol should reply with a success message
        self.proto.dataReceived(self.packer.pack(record_msg))

        self.unpacker.feed(self.transport.value())
        self.transport.clear()

        reply = next(self.unpacker)
        rtype, rmsg = validate_message(reply)
        self.assertEqual(rtype, 'status')
        self.assertTrue(rmsg['success'])
        self.assertEqual(rmsg['info']['recorded'], 2)

#
#
# class Counter:
#     def __init__(self):
#         self._count = 0
#
#     @property
#     def count(self) -> int:
#         return self._count
#
#     def inc(self) -> None:
#         self._count += 1
#
#
# class DynamicTestsMeta(type):
#     def __init__(cls, *args, **kwargs):
#         super(DynamicTestsMeta, cls).__init__(*args, **kwargs)
#
#         def make_test(msg_type: str) -> Callable:
#             def _test(self: TestProtocol):
#                 calls = Counter()
#
#                 # register a handler for the msg_type
#                 @self.proto.handler(msg_type)
#                 def handler(payload):
#                     calls.inc()
#
#                 # send a valid message, check that call counter has been
#                 # incremented
#                 valid_msg = {
#                     'type'   : msg_type,
#                     'payload': valid_payloads[msg_type]
#                 }
#                 self.proto.dataReceived(self.packer.pack(valid_msg))
#                 self.assertEqual(calls.count, 1)
#
#                 # check that reply is valid
#                 self.unpacker.feed(self.transport.value())
#                 reply = next(self.unpacker)
#                 self.transport.clear()
#
#                 mt, pload = validate_message(reply)
#                 self.assertEqual(mt, 'status')
#                 self.assertTrue(pload['success'])
#
#                 # check handler is not called for invalid messages
#                 invalid_msg = {
#                     'type'   : 'invalid',
#                     'payload': {}
#                 }
#                 self.proto.dataReceived(self.packer.pack(invalid_msg))
#                 self.assertEqual(calls.count, 1)
#
#                 # check that protocol sends an error reply
#                 self.unpacker.feed(self.transport.value())
#                 err_reply = next(self.unpacker)
#                 self.transport.clear()
#
#                 mt, pload = validate_message(err_reply)
#                 self.assertEqual(mt, 'status')
#                 self.assertFalse(pload['success'])
#
#             return _test
#
#         for msg_type in valid_payloads.keys():
#             setattr(cls, f'test_{msg_type}', make_test(msg_type))
#
#
# class TestProtocol(unittest.TestCase, metaclass=DynamicTestsMeta):
#     def setUp(self) -> None:
#         addr = ('0.0.0.0', 0)
#         factory = Factory.forProtocol(MessageProtocol)
#         self.proto: MessageProtocol = factory.buildProtocol(addr)
#         self.transport = proto_helpers.StringTransport()
#         self.proto.makeConnection(self.transport)
#
#         # use the special packers
#         self.packer = MessagePacker()
#         self.unpacker = MessageUnpacker()
#
#         # first thing server should send is version
#         self.unpacker.feed(self.transport.value())
#         version_msg = next(self.unpacker)
#
#         msg_type, msg = validate_message(version_msg)
#         self.assertEqual(msg_type, 'version')
#         self.assertEqual(msg['major'], self.proto.version_major)
#         self.assertEqual(msg['minor'], self.proto.version_minor)
#
#         self.transport.clear()
