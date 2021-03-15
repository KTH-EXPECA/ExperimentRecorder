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
from pathlib import Path

import toml
from schema import SchemaError
from twisted.trial import unittest

from exprec.config import validate_config

valid_toml_config = '''
[experiment]
name = "TestValid"
description = "Valid Test Configuration"

[database]
path = "/:memory:"
persist = true

[socket]
type = "tcp4"
interface = "0.0.0.0"
port = 1337

[output]
directory = "/tmp/output"
table_filetype = "parquet"

'''

invalid_toml_config = '''
[experiment]
name = "TestInvalid"
description = "Invalid Test Configuration"

[output]
directory = "/tmp/output"
table_filetype = 3

'''


class TestConfigValidation(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_valid_TOML_config(self):
        config = toml.loads(valid_toml_config)
        config = validate_config(config)

        # check that default value is there for unspecified key: backlog
        self.assertIn('backlog', config['socket'])

        # check paths
        self.assertIsInstance(config['database']['path'], Path)
        self.assertIsInstance(config['output']['directory'], Path)

    def test_invalid_TOML_config(self):
        config = toml.loads(invalid_toml_config)
        with self.assertRaises(SchemaError):
            validate_config(config)
