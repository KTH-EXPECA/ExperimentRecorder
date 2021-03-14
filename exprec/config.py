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
from typing import Any, Mapping

from schema import Optional, Or, Schema

_CONFIG_SCHEMA = Schema(
    {
        'experiment': {
            'name'                             : str,
            Optional('description', default=''): str
        },
        Optional('database', default={
            'uri'    : 'sqlite:///tmp/exp.db',
            'persist': False
        })          : {
            'uri'    : str,
            'persist': Or(True, False, only_one=True)
        },
        'socket'    : {
            'type'                         : Or('unix', 'tcp', only_one=True),
            'bind'                         : str,
            Optional('backlog', default=50): int
        },
        'output'    : {
            'directory'                              : str,
            Optional('table_filetype', default='csv'):
                Or('csv', 'parquet', only_one=True)
        }
    }
)


def validate_config(config: Any) -> Mapping[str, Any]:
    return _CONFIG_SCHEMA.validate(config)
