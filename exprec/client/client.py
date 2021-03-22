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
from __future__ import annotations

from collections import deque

from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.logger import Logger
from twisted.python.failure import Failure

from ..common.messages import InvalidMessageError, ValidMessage, make_message, \
    validate_message
from ..common.packing import *
from ..common.protocol import ProtocolException, \
    check_message_type


class ExperimentClient(Protocol):
    #: protocol version
    #: TODO: in the future, deal with version mismatch
    version_major = 1
    version_minor = 0

    def __init__(self, exp_id_deferred: Deferred = Deferred()):
        super(ExperimentClient, self).__init__()
        self._logger = Logger()
        self._waiting = deque()
        self._packer = MessagePacker()
        self._unpacker = MessageUnpacker()

        self._exp_id_d = exp_id_deferred

    def connectionMade(self):
        # as soon as connection is made, send version message
        msg = make_message('version',
                           {
                               'major': self.version_major,
                               'minor': self.version_minor
                           })

        @check_message_type('welcome')
        def welcome_callback(msg: ValidMessage):
            self._exp_id_d.callback(msg.payload['instance_id'])

        d = Deferred()
        d.addCallback(welcome_callback)
        self._waiting.append(d)

        self.transport.write(self._packer.pack(msg))

    def dataReceived(self, data: bytes):
        self._unpacker.feed(data)
        for msg in self._unpacker:
            try:
                handler_d = self._waiting.popleft()

                base_d = Deferred()
                base_d.addCallback(validate_message)
                base_d.chainDeferred(handler_d)
                base_d.addErrback(self._invalid_msg_errback)
                base_d.addErrback(self._fallback_msg_errback)

                base_d.callback(msg)
            except IndexError:
                self._logger.error(
                    format='Received a message when not expecting one?'
                )

    @property
    def backlog(self) -> int:
        return len(self._waiting)

    def _invalid_msg_errback(self, fail: Failure):
        fail.trap(InvalidMessageError)
        self._logger.error(
            format='Received an invalid message!',
        )

    def _fallback_msg_errback(self, fail: Failure):
        self._logger.critical(
            format='Exception in message handling callback.'
        )
        self.transport.loseConnection()
        fail.trap()

    @check_message_type('status')
    def _waiting_status_callback(self, msg: ValidMessage) -> None:
        if not msg.payload['success']:
            cause = msg.payload.get('error', 'unknown')
            self._logger.critical(
                format='Received error status! Cause: {cause}',
                cause=cause
            )
            raise ProtocolException(f'Error status. Cause: {cause}')

        return None

    def _send(self, msg: Mapping[str, Any]) -> Deferred:
        d = Deferred()
        d.addCallback(self._waiting_status_callback)
        self._waiting.append(d)

        data = self._packer.pack(msg)
        self.transport.write(data)
        return d

    def write_metadata(self, **kwargs) -> Deferred:
        msg = make_message(
            msg_type='metadata',
            payload=kwargs
        )
        return self._send(msg)

    def record_variables(self,
                         timestamp: datetime.datetime,
                         **kwargs) -> Deferred:
        msg = make_message(
            msg_type='record',
            payload={
                'timestamp': timestamp,
                'variables': kwargs
            }
        )
        return self._send(msg)

    def finish(self) -> None:
        msg = make_message('finish', None)
        self.transport.write(self._packer.pack(msg))
        self.transport.loseConnection()
