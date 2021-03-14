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
from sqlalchemy.orm import Session
from twisted.internet.protocol import Factory, Protocol

from .messages import InvalidMessageError, make_message, validate_message
from .models import ExperimentInstance


# msgpack needs special code to pack/unpack datetimes and uuids
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

    def __init__(self, session: Session):
        super(MessageProtocol, self).__init__()
        self._unpacker = MessageUnpacker()
        self._packer = MessagePacker()

        self._session = session

        self._exp_instance = ExperimentInstance()

        self._session.add(self._exp_instance)
        self._session.commit()

    def connectionMade(self):
        # send version message and instance id
        version_msg = make_message('version', {
            'major': self.version_major,
            'minor': self.version_minor
        })
        self._send(version_msg)

        inst_msg = make_message('welcome', {'instance_id': self._instance.id})
        self._send(inst_msg)

    # noinspection PyArgumentList
    def _send(self, o: Any) -> None:
        self.transport.write(self._packer.pack(o))

    def dataReceived(self, data: bytes):
        self._unpacker.feed(data)
        for msg in self._unpacker:
            try:
                msg_type, payload = validate_message(msg)
                assert msg_type == 'record'
            except (InvalidMessageError, AssertionError):
                error_msg = make_message('status',
                                         {'error': 'Invalid message.'})
                self._send(error_msg)
                continue

            for var_name, var_value in payload.items():



class MessageProtoFactory(Factory):
    def __init__(self, session: Session):
        self._db_session = session

    def buildProtocol(self, addr: Tuple[str, int]) -> Protocol:
        return MessageProtocol(self._db_session)
