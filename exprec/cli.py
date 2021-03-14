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
from typing import TextIO

import click
# TODO add logging
import toml

from .config import validate_config


@click.command()
@click.option('-c', '--config-file',
              type=click.File(mode='r'),
              default='./exprec_config.toml',
              show_default=True,
              help='Configuration file.')
def main(config_file: TextIO) -> None:
    # load config
    config = toml.load(config_file)
    config = validate_config(config)

