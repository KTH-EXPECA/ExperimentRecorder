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

from typing import Any, Mapping

from twisted.internet.address import IPv4Address, IPv6Address, UNIXAddress
from twisted.internet.interfaces import IAddress
from twisted.internet.protocol import Factory, Protocol, connectionDone
from twisted.python import failure

from .exp_interface import BufferedExperimentInterface
# msgpack needs special code to pack/unpack datetimes and uuids
from ..common.messages import InvalidMessageError, make_message, \
    validate_message
from ..common.packing import MessagePacker, MessageUnpacker


# ---

class MessageProtocol(Protocol):
    #: protocol version
    #: TODO: in the future, deal with version mismatch
    version_major = 1
    version_minor = 0

    def __init__(self,
                 interface: BufferedExperimentInterface,
                 addr: IAddress):
        # TODO: document

        super(MessageProtocol, self).__init__()
        self._unpacker = MessageUnpacker()
        self._packer = MessagePacker()

        self._interface = interface
        self._experiment_id = self._interface.new_experiment_instance()
        self._addr = addr

        # immediately add the ip address to the newly created experiment
        # instance
        if isinstance(addr, UNIXAddress):
            address = str(addr.name)
        elif isinstance(addr, (IPv4Address, IPv6Address)):
            address = f'{addr.host}:{addr.port}'
        else:
            # TODO: warning
            address = ''

        self._interface.add_metadata(
            experiment_id=self._experiment_id,
            address=address.lower()
        )

    def connectionMade(self):
        # send version message and instance id
        version_msg = make_message('version', {
            'major': self.version_major,
            'minor': self.version_minor
        })
        self._send(version_msg)

        inst_msg = make_message('welcome', {'instance_id': self._experiment_id})
        self._send(inst_msg)

    def connectionLost(self, reason: failure.Failure = connectionDone):
        # multiple connections share same interface, don't close it
        # however, add a timestamp to the experiment
        self._interface.finish_experiment_instance(self._experiment_id)

    # noinspection PyArgumentList
    def _send(self, o: Any) -> None:
        self.transport.write(self._packer.pack(o))

    def _handle_metadata_msg(self, msg: Mapping[str, str]) -> None:
        self._interface.add_metadata(experiment_id=self._experiment_id, **msg)
        ret_msg = make_message('status', {'success': True})
        self._send(ret_msg)

    def _handle_record_msg(self, msg: Mapping[str, Any]) -> None:
        timestamp = msg['timestamp']
        variables = msg['variables']
        self._interface.record_variables(
            experiment_id=self._experiment_id,
            timestamp=timestamp,
            **variables
        )
        ret_msg = make_message('status', {
            'success': True,
            'info'   : {'recorded': len(variables)}
        })
        self._send(ret_msg)

    def dataReceived(self, data: bytes):
        self._unpacker.feed(data)
        for msg in self._unpacker:
            try:
                msg_type, payload = validate_message(msg)
                if msg_type == 'metadata':
                    self._handle_metadata_msg(payload)
                elif msg_type == 'record':
                    self._handle_record_msg(payload)
                else:
                    raise InvalidMessageError()
            except InvalidMessageError:
                error_msg = make_message('status',
                                         {'error': 'Invalid message.'})
                self._send(error_msg)
                continue


class MessageProtoFactory(Factory):
    def __init__(self, interface: BufferedExperimentInterface):
        self._interface = interface

    def buildProtocol(self, addr: IAddress) -> MessageProtocol:
        return MessageProtocol(self._interface, addr)
