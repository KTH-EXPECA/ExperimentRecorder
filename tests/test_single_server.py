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

import time

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from twisted.internet.address import IPv4Address
from twisted.logger import eventAsText, globalLogPublisher
from twisted.test import proto_helpers
from twisted.trial import unittest

from exprec.common.messages import make_message, validate_message
from exprec.common.packing import MessagePacker, MessageUnpacker
from exprec.server.exp_interface import BufferedExperimentInterface
from exprec.server.models import *
from exprec.server.protocol import SingleExperimentServer


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
        # add a logger to check what's going on
        def log_observer(e):
            print(eventAsText(e))

        self._obs = log_observer
        globalLogPublisher.addObserver(self._obs)

        addr = IPv4Address(type='TCP', host='localhost', port=1312)
        self._engine = create_engine(
            'sqlite:///:memory:',
            connect_args={'check_same_thread': False},
            poolclass=StaticPool)

        self._interface = BufferedExperimentInterface(db_engine=self._engine)
        self.proto = SingleExperimentServer(self._interface, addr)
        self.transport = proto_helpers.StringTransport()
        self.proto.makeConnection(self.transport)

        # use the special packers
        self.packer = MessagePacker()
        self.unpacker = MessageUnpacker()

        # client needs to first send a valid version
        msg = make_message('version',
                           {
                               'major': self.proto.version_major,
                               'minor': self.proto.version_minor
                           })
        self.proto.dataReceived(self.packer.pack(msg))

        # server responds with the welcome message
        self.unpacker.feed(self.transport.value())
        self.transport.clear()

        welcome_msg = next(self.unpacker)
        msg_type, msg = validate_message(welcome_msg)
        self.assertEqual(msg_type, 'welcome')
        self.assertIn('instance_id', msg)

        # a new metadata instance should exist for the experiment instance
        session = self._interface.session

        mdata = session \
            .query(ExperimentMetadata) \
            .filter(ExperimentMetadata.instance_id == msg['instance_id']) \
            .first()

        self.assertIsNotNone(mdata)
        self.assertEqual(mdata.label, 'address')

    def tearDown(self) -> None:
        # make sure the protocol shuts down
        msg = make_message('finish', None)
        self.proto.dataReceived(self.packer.pack(msg))
        self.transport.clear()
        self._interface.close()
        globalLogPublisher.removeObserver(self._obs)

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

    def test_repeat_send_record(self):
        for _ in range(10):
            self.test_send_record()

    def test_repeat_send_metadata(self):
        for _ in range(10):
            self.test_send_metadata()
