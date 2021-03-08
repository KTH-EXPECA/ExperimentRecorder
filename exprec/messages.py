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
from typing import Any, Mapping, NamedTuple, Tuple

from schema import Optional, Or, Schema, SchemaError

_msg_payload_schemas = {
    'version': {
        'major': int,
        'minor': int
    },
    'init'   : {
        'experiment_id': str,
        'variables'    : {
            str: Or(type(int), type(float), type(bool))
        }
    },
    'finish' : {
        'experiment_id': str
    },
    'record' : {
        'experiment_id': str,
        'variables'    : {
            str: {'value': Or(int, float, bool), 'timestamp': float}
        }
    },
    'status' : {
        'success'                         : bool,
        Optional(Or('info', 'error', only_one=True)): str
    }
}


class InvalidMessageError(Exception):
    pass


class _ValidMessage(NamedTuple):
    mtype: str
    payload: Mapping[str, Any]


def validate_message(msg: Mapping[str, Any]) \
        -> Tuple[str, Mapping[str, Any]]:
    try:
        mtype = msg['type']
        payload = msg['payload']
        payload_schema = _msg_payload_schemas[mtype]

        return _ValidMessage(mtype, Schema(payload_schema).validate(payload))
    except (KeyError, SchemaError):
        raise InvalidMessageError(msg)


def make_message(msg_type: str,
                 payload: Mapping[str, Any]) -> Mapping[str, Any]:
    mtype, payload = validate_message({'type': msg_type, 'payload': payload})
    return {
        'type'   : mtype,
        'payload': payload
    }


MESSAGE_TYPES = set(_msg_payload_schemas.keys())
