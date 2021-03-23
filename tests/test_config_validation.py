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
from sqlalchemy.engine import Engine
from twisted.trial import unittest

from exprec.server.config import validate_config

valid_toml_config = '''
[experiment]
    name = "TestValid"
    description = "Valid Test Configuration"
    
[experiment.default_metadata]
    potatoes = "boil 'em, mash 'em, stick 'em in a stew"

[output]
    directory = "/tmp/exprec"
    record_file = "test_records.csv"
    metadata_file = "test_metadata.json"
    times_file = "test_times.json"
    
[database]
    engine = "sqlite:///:memory:"
    persist = true
    record_chunksize = 128

[server]
    endpoint = "tcp6:1312:interface=0.0.0.0"
    
'''


class TestConfigValidation(unittest.TestCase):
    def test_valid_TOML_config(self):
        config = toml.loads(valid_toml_config)
        config = validate_config(config)

        # check keys that should have been converted
        self.assertIn('output', config)
        self.assertIn('directory', config['output'])
        self.assertIsInstance(config['output']['directory'], Path)

        self.assertIn('database', config)
        self.assertIn('engine', config['database'])
        self.assertIsInstance(config['database']['engine'], Engine)

        self.assertIn('server', config)
        self.assertIn('endpoint', config['server'])
        self.assertTrue(hasattr(config['server']['endpoint'], 'listen'))

    def test_invalid_output_dir(self):
        config = toml.loads(valid_toml_config)
        config['output']['directory'] = '/bin/bash'
        with self.assertRaises(SchemaError):
            validate_config(config)

    def test_invalid_db_engine(self):
        config = toml.loads(valid_toml_config)
        config['database']['engine'] = 'tcp:8000'
        with self.assertRaises(SchemaError):
            validate_config(config)

    def test_invalid_server_endpoint(self):
        config = toml.loads(valid_toml_config)
        config['server']['endpoint'] = '/home/molguin'
        with self.assertRaises(SchemaError):
            validate_config(config)
