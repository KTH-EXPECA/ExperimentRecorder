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

import time

from twisted.internet.protocol import Protocol

from ..common.messages import InvalidMessageError, make_message, \
    validate_message
from ..common.packing import *


class ExperimentClock:
    def __init__(self):
        self._start_time = time.monotonic()

    def time(self) -> float:
        return time.monotonic() - self._start_time

    def datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.time())


class UninitializedExperiment(Exception):
    pass


class ProtocolException(Exception):
    pass


class IncompatibleVersionException(ProtocolException):
    pass


class ExpRecClient(Protocol):
    #: protocol version
    #: TODO: in the future, deal with version mismatch
    version_major = 1
    version_minor = 0

    def __init__(self):
        self._packer = MessagePacker()
        self._unpacker = MessageUnpacker()
        self._waiting_for_status = 0

        def uninit_experiment_id() -> uuid.UUID:
            raise UninitializedExperiment()

        def uninit_metadata(**kwargs) -> None:
            raise UninitializedExperiment()

        def uninit_record_vars(**kwargs) -> None:
            raise UninitializedExperiment()

        self.get_experiment_id = uninit_experiment_id
        self.write_metadata = uninit_metadata
        self.record_variables = uninit_record_vars

        def _handshake(msg_type: str, msg_payload: Mapping[str, Any]) -> None:
            if msg_type == 'welcome':
                exp_id = msg_payload['instance_id']
                exp_clock = ExperimentClock()

                # make new handlers
                def _experiment_id() -> uuid.UUID:
                    return exp_id

                def _write_metadata(**kwargs) -> None:
                    msg = make_message(
                        msg_type='metadata',
                        payload=kwargs
                    )
                    self._send(msg)
                    self._waiting_for_status += 1

                def _record_variables(**kwargs) -> None:
                    msg = make_message(
                        msg_type='record',
                        payload={
                            'timestamp': exp_clock.datetime(),
                            'variables': kwargs
                        }
                    )
                    self._send(msg)
                    self._waiting_for_status += 1

                def _handshake_handler(*args, **kwargs) -> None:
                    # fully disable handshake handler after handshake is done
                    raise ProtocolException()

                self.get_experiment_id = _experiment_id
                self.write_metadata = _write_metadata
                self.record_variables = _record_variables
                self._handshake_handler = _handshake_handler
            else:
                raise ProtocolException()

        self._handshake_handler = _handshake

    def _send(self, msg: Mapping[str, Any]):
        data = self._packer.pack(msg)
        self.transport.write(data)

    def dataReceived(self, data: bytes):
        try:
            self._unpacker.feed(data)
            for msg in self._unpacker:
                try:
                    msg_type, payload = validate_message(msg)
                    if msg_type == 'version':
                        if self.version_major != payload['major']:
                            raise IncompatibleVersionException()
                            # TODO: handle versions in the future
                    elif msg_type == 'status':
                        if self._waiting_for_status > 0:
                            self._waiting_for_status -= 1
                        else:
                            raise ProtocolException()
                    else:
                        self._handshake_handler(msg_type, payload)
                except InvalidMessageError as e:
                    # TODO log and ignore
                    pass
        except:
            # any exception causes disconnection and then reraises
            self.transport.loseConnection()
            raise
