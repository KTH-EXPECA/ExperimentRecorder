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
import json
from pathlib import Path
from typing import Any, Collection, Mapping, TextIO

import click
import pandas as pd
# TODO add logging
import toml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ServerEndpoint, TCP6ServerEndpoint, \
    UNIXServerEndpoint
from twisted.internet.posixbase import PosixReactorBase

from .config import validate_config
from .exp_interface import BufferedExperimentInterface
from .models import *
from .protocol import MessageProtoFactory

reactor: PosixReactorBase = reactor


def _records_to_dataframe(db_session: Session,
                          exp_ids: Collection[uuid.UUID]) -> pd.DataFrame:
    # collects all variable records into a table
    # Base.metadata.create_all(engine)
    # session_fact = sessionmaker(bind=engine)
    # session = session_fact()

    # set up tables
    query = db_session.query(
        ExperimentInstance.id.label('experiment'),
        InstanceVariable.name.label('variable'),
        VariableRecord.timestamp.label('timestamp'),
        VariableRecord.value.label('value')
    ).filter(ExperimentInstance.id.in_(exp_ids)) \
        .filter(InstanceVariable.instance_id == ExperimentInstance.id) \
        .filter(VariableRecord.variable_id == InstanceVariable.id) \
        .all()

    df = pd.DataFrame([r._asdict() for r in query])
    # session.close()
    if not df.empty:
        df = df.pivot(index=['experiment', 'timestamp'],
                      columns='variable',
                      values='value')
    return df


def _metadata_to_json(session: Session,
                      exp_ids: Collection[uuid.UUID]) \
        -> Mapping[str, Any]:
    # returns the experiment metadata in the DB as a json-compliant dict
    output = {}
    for exp_id in exp_ids:
        query = session \
            .query(ExperimentInstance.id,
                   ExperimentMetadata.label,
                   ExperimentMetadata.value) \
            .filter(ExperimentInstance.id == exp_id) \
            .filter(ExperimentInstance.id == ExperimentMetadata.instance_id) \
            .all()

        output[str(exp_id)] = {q.label: q.value for q in query}
    return output


def _aggregate_and_output(engine: Engine,
                          exp_ids: Collection[uuid.UUID],
                          output_path: Path):
    # first, get data as a table and metadata as a dict
    Base.metadata.create_all(engine)
    session_fact = sessionmaker(bind=engine)
    session = session_fact()

    data = _records_to_dataframe(session, exp_ids)
    metadata = _metadata_to_json(session, exp_ids)

    # output to files
    with (output_path / 'metadata.json').open('w') as fp:
        json.dump(obj=metadata, fp=fp, indent=4)
    data.to_csv(output_path / 'data.csv', index=True)


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

    # initialize engine
    engine = create_engine(f'sqlite://{config["database"]["path"]}',
                           connect_args={'check_same_thread': False},
                           poolclass=StaticPool)
    interface = BufferedExperimentInterface(
        db_engine=engine,
        default_metadata={
            'experiment_name': config['experiment']['name'],
            'experiment_desc': config['experiment']['description']
        })
    protocol_fact = MessageProtoFactory(interface)

    # ready to listen on whatever the config says
    sock_cfg = config['socket']
    if sock_cfg['type'] == 'unix':
        # listen on a unix socket
        endpoint = UNIXServerEndpoint(reactor=reactor,
                                      address=sock_cfg['path'],
                                      backlog=sock_cfg['backlog'])
    elif sock_cfg['type'] == 'tcp4':
        endpoint = TCP4ServerEndpoint(reactor=reactor,
                                      interface=sock_cfg['interface'],
                                      port=sock_cfg['port'],
                                      backlog=sock_cfg['backlog'])
    elif sock_cfg['type'] == 'tcp6':
        endpoint = TCP6ServerEndpoint(reactor=reactor,
                                      interface=sock_cfg['interface'],
                                      port=sock_cfg['port'],
                                      backlog=sock_cfg['backlog'])
    else:
        # should never happen
        raise RuntimeError(f'Invalid socket type {sock_cfg["type"]}.')

    endpoint.listen(protocol_fact)

    def _shutdown():
        # on shutdown, we close the interface and write out to the CSV file
        interface.close()
        _aggregate_and_output(engine=engine,
                              exp_ids=interface.experiment_instances,
                              output_path=config['output']['directory'])

        # finally, delete the db file if needed
        if not config['database']['persist']:
            config['database']['path'].unlink(missing_ok=True)

    # clean shutdown
    reactor.addSystemEventTrigger('before', 'shutdown', _shutdown)

    reactor.run()
