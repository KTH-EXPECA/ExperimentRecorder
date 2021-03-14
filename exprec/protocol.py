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

import datetime
import uuid
from typing import Any, Mapping, Tuple

import msgpack
from twisted.internet.protocol import Factory, Protocol

from .exp_interface import BufferedExperimentInterface
# msgpack needs special code to pack/unpack datetimes and uuids
from .messages import InvalidMessageError, make_message, validate_message


class MessagePacker(msgpack.Packer):
    @staticmethod
    def encode_vartype(obj: Any) -> Mapping[str, Any]:
        if isinstance(obj, datetime.datetime):
            return {'__date__': obj.timestamp()}
        elif isinstance(obj, uuid.UUID):
            return {'__uuid__': f'{obj.int:32x}'}
        else:
            return obj

    def __init__(self, *args, **kwargs):
        kwargs['default'] = self.encode_vartype
        super(MessagePacker, self).__init__(*args, **kwargs)


class MessageUnpacker(msgpack.Unpacker):
    @staticmethod
    def decode_vartype(data: Any) -> Any:
        if '__date__' in data:
            return datetime.datetime.fromtimestamp(data['__date__'])
        elif '__uuid__' in data:
            return uuid.UUID(data['__uuid__'])
        else:
            return data

    def __init__(self, *args, **kwargs):
        kwargs['object_hook'] = self.decode_vartype
        super(MessageUnpacker, self).__init__(*args, **kwargs)


# ---

class MessageProtocol(Protocol):
    #: protocol version
    #: TODO: in the future, deal with version mismatch
    version_major = 1
    version_minor = 0

    def __init__(self, interface: BufferedExperimentInterface):
        super(MessageProtocol, self).__init__()
        self._unpacker = MessageUnpacker()
        self._packer = MessagePacker()

        self._interface = interface
        self._experiment_id = self._interface.new_experiment_instance()

    def connectionMade(self):
        # send version message and instance id
        version_msg = make_message('version', {
            'major': self.version_major,
            'minor': self.version_minor
        })
        self._send(version_msg)

        inst_msg = make_message('welcome', {'instance_id': self._experiment_id})
        self._send(inst_msg)

    # multiple connections share same interface, don't close it
    # def connectionLost(self, reason: failure.Failure = connectionDone):
    #     self._interface.close()

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

    def buildProtocol(self, addr: Tuple[str, int]) -> MessageProtocol:
        return MessageProtocol(self._interface)
