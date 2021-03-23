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
from pathlib import Path
from typing import Any, Mapping

from schema import Optional, Or, Schema, Use
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool
from twisted.internet import reactor
from twisted.internet.endpoints import serverFromString


def validate_dir_path(d: str) -> Path:
    path = Path(d)
    path.mkdir(parents=True, exist_ok=True)
    return path


def validate_db_engine(s: str) -> Engine:
    return create_engine(s,
                         connect_args={'check_same_thread': False},
                         poolclass=StaticPool)


_CONFIG_SCHEMA = Schema(
    {
        'experiment': {
            'name'                                  : str,
            Optional('description', default='')     : str,
            Optional('default_metadata', default={}): {Optional(str): str}
        },
        'output'    : {
            'directory'                      : Use(validate_dir_path),
            Optional('record_file',
                     default='records.csv')  : str,
            Optional('metadata_file',
                     default='metadata.json'): str,
            Optional('times_file',
                     default='times.json')   : str
        },
        'database'  : {
            'engine'              : Use(validate_db_engine),
            Optional('record_chunksize',
                     default=1000): int
        },
        'server'    : {
            'endpoint': Use(lambda s: serverFromString(reactor, s))
        }
    }
)


def validate_config(config: Any) -> Mapping[str, Any]:
    return _CONFIG_SCHEMA.validate(config)
