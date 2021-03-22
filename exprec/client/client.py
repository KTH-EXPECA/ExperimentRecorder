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
from ..common.protocol import IncompatibleVersionException, ProtocolException, \
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

        # startup callbacks
        @check_message_type('welcome')
        def _welcome_callback(msg: ValidMessage) -> None:
            exp_id = msg.payload['instance_id']
            self._logger.info(
                'Initialized and assigned experiment ID {exp_id}.',
                exp_id=exp_id
            )
            return exp_id_deferred.callback(exp_id)

        @check_message_type('version')
        def _version_callback(msg: ValidMessage) -> None:
            if msg.payload['major'] > self.version_major:
                raise IncompatibleVersionException()

        version_d = Deferred()
        version_d.addCallback(_version_callback)

        welcome_d = Deferred()
        welcome_d.addCallback(_welcome_callback)

        self._waiting.extend([version_d, welcome_d])

    @property
    def backlog(self) -> int:
        return len(self._waiting)

    def dataReceived(self, data: bytes):
        self._unpacker.feed(data)
        for msg in self._unpacker:
            try:
                valid_msg = validate_message(msg)
                handler_d = self._waiting.popleft()
                handler_d.addErrback(self._invalid_msg_errback)
                handler_d.addErrback(self._fallback_msg_errback)
                handler_d.callback(valid_msg)
            except IndexError:
                self._logger.error(
                    format='Received a message when not expecting one?'
                )

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
