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
import sys
from typing import TextIO

import click
import loguru
import toml
from twisted.internet import reactor
from twisted.internet.posixbase import PosixReactorBase

from .config import validate_config
from .logging import LoggingThread
from .protocol import ExperimentRecordingServer
from .._version import __version__

reactor: PosixReactorBase = reactor


def _verbose_count_to_loguru_level(verbose: int) -> int:
    return max(loguru.logger.level('CRITICAL').no - (verbose * 10), 0)


def _configure_loguru_sink(verbose: int) -> None:
    # configure the loguru sink
    loguru.logger.remove()  # remove the default one and replace it
    loguru.logger.add(sys.stderr,
                      level=_verbose_count_to_loguru_level(verbose),
                      colorize=True,
                      format='<light-green>{time}</light-green> '
                             '<level><b>{level}</b></level> '
                             '{message}')


@click.command(context_settings={'help_option_names': ['-h', '--help']})
@click.argument('config-file', type=click.File(mode='r'))
@click.version_option(version=__version__, prog_name='ExpRec Server')
@click.option('-v', '--verbose', count=True, default=0, show_default=False,
              help='Set the STDERR logging verbosity level.')
def main(config_file: TextIO, verbose: int) -> None:
    # TODO DOC

    # set up nice concurrent logging
    _configure_loguru_sink(verbose)
    logging_thread = LoggingThread()
    logging_thread.start()

    # load config from the specified TOML file
    config = validate_config(toml.load(config_file))
    server = ExperimentRecordingServer(
        db_engine=config['database']['engine'],
        output_dir=config['output']['directory'],
        record_chunk_size=config['database']['record_chunksize'],
        records_filename=config['output']['record_file'],
        metadata_filename=config['output']['metadata_file'],
        times_filename=config['output']['times_file'],
        default_metadata=config['experiment']['default_metadata']
    )

    # ready to listen on whatever the config says
    config['server']['endpoint'].listen(server)  # start listening
    reactor.run()
    logging_thread.join()  # needs to go after the .run
