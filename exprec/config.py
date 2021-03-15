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
import socket
from pathlib import Path
from typing import Any, Literal, Mapping

from schema import Optional, Or, Schema, SchemaError, Use


def _validate_ip_addr(family: Literal['ipv4', 'ipv6'], addr: str):
    if family == 'ipv4':
        socket.inet_pton(socket.AF_INET, addr)
    elif family == 'ipv6':
        socket.inet_pton(socket.AF_INET6, addr)
    else:
        raise SchemaError('Invalid IP address.')
    return addr


def _validate_dir_path(d: str) -> Path:
    path = Path(d)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _validate_new_file_path(p: str) -> Path:
    path = Path(p)
    assert not path.exists()
    return path


_CONFIG_SCHEMA = Schema(
    {
        'experiment': {
            'name'                             : str,
            Optional('description', default=''): str
        },
        Optional('database', default={
            'path'    : '/tmp/exp.db',
            'persist': False
        })          : {
            'path'    : Use(Path),
            'persist': Or(True, False, only_one=True)
        },
        'socket'    : Or(  # either a tcp4, tcp6 or UNIX socket
            {
                'type'                         : 'tcp4',
                'interface'                    : Use(
                    lambda addr: _validate_ip_addr(family='ipv4',
                                                   addr=addr)),
                'port'                         : int,
                Optional('backlog', default=50): int
            },
            {
                'type'                         : 'tcp6',
                'interface'                    : Use(
                    lambda addr: _validate_ip_addr(family='ipv6',
                                                   addr=addr)),
                'port'                         : int,
                Optional('backlog', default=50): int
            },
            {
                'type'                         : 'unix',
                'path'                         : Use(_validate_new_file_path),
                Optional('backlog', default=50): int
            },
            only_one=True
        ),
        'output'    : {
            'directory'                              :
                Use(_validate_dir_path),
            Optional('table_filetype', default='csv'):
                Or('csv', 'parquet', only_one=True)
        }
    }
)


def validate_config(config: Any) -> Mapping[str, Any]:
    return _CONFIG_SCHEMA.validate(config)
