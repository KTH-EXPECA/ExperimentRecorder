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
import datetime
import uuid
from typing import Any, Mapping

import msgpack


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
