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
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Mapping

from sqlalchemy.engine import Engine
from twisted.internet.address import IPv4Address, IPv6Address, UNIXAddress
from twisted.internet.defer import Deferred
from twisted.internet.interfaces import IAddress
from twisted.internet.protocol import Factory, Protocol
from twisted.internet.task import LoopingCall
from twisted.logger import Logger
from twisted.python.failure import Failure

from .exp_interface import BufferedExperimentInterface
# msgpack needs special code to pack/unpack datetimes and uuids
from ..common.messages import make_message, \
    validate_message
from ..common.packing import MessagePacker, MessageUnpacker
from ..common.protocol import *


class SingleExperimentServer(Protocol):
    #: protocol version
    #: TODO: in the future, deal with version mismatch
    version_major = 1
    version_minor = 0

    def __init__(self,
                 interface: BufferedExperimentInterface,
                 addr: IAddress):
        # TODO: document

        super(SingleExperimentServer, self).__init__()
        self._unpacker = MessageUnpacker()
        self._packer = MessagePacker()
        self._interface = interface
        self._addr = addr
        self._log = Logger()

        @check_message_type('version')
        def wait_for_version(msg: ValidMessage):
            v_major = msg.payload['major']
            v_minor = msg.payload['minor']
            if self.version_major != v_major:
                raise IncompatibleVersionException(
                    server_version=(self.version_major, self.version_minor),
                    client_version=(v_major, v_minor)
                )
            else:
                # send welcome message
                experiment_id = self._interface.new_experiment_instance()
                self._log.info('Assigned experiment id {exp_id} to client '
                               'with address {addr}.',
                               exp_id=experiment_id, addr=addr)

                # immediately add the ip address to the newly created
                # experiment instance
                if isinstance(addr, UNIXAddress):
                    address = str(addr.name)
                elif isinstance(addr, (IPv4Address, IPv6Address)):
                    address = f'{addr.host}:{addr.port}'
                else:
                    self._log.warning(
                        format='Could not obtain address for client!'
                    )
                    address = ''

                self._interface.add_metadata(
                    experiment_id=experiment_id,
                    address=address.lower()
                )

                welcome_msg = make_message(
                    msg_type='welcome',
                    payload={'instance_id': experiment_id}
                )
                self._send(welcome_msg)

                self.wait_for_records_metadata_or_finish(exp_id=experiment_id)

        def errback(fail: Failure):
            # something failed while waiting for version, just drop the conn
            self._log.failure(fail)
            self.transport.loseConnection()

        self._current_d = Deferred() \
            .addCallback(validate_message) \
            .addCallback(wait_for_version) \
            .addErrback(errback)

    def wait_for_records_metadata_or_finish(self, exp_id: uuid.UUID):
        @check_message_type('record', 'metadata', 'finish')
        def callback(msg: ValidMessage):
            if msg.mtype == 'record':
                timestamp = msg.payload['timestamp']
                variables = msg.payload['variables']
                self._interface.record_variables(
                    experiment_id=exp_id,
                    timestamp=timestamp,
                    **variables
                )
                ret_msg = make_message('status', {
                    'success': True,
                    'info'   : {'recorded': len(variables)}
                })
                self._send(ret_msg)
                self.wait_for_records_metadata_or_finish(exp_id=exp_id)

            elif msg.mtype == 'metadata':
                self._interface.add_metadata(
                    experiment_id=exp_id, **msg.payload
                )
                ret_msg = make_message('status', {'success': True})
                self._send(ret_msg)
                self.wait_for_records_metadata_or_finish(exp_id=exp_id)

            elif msg.mtype == 'finish':
                # shut down this thing
                self._log.warn(
                    format='Shutting down server for experiment {exp_id}.',
                    exp_id=exp_id
                )
                self._interface.finish_experiment_instance(exp_id)
                self.transport.loseConnection()

        def errback(fail: Failure):
            # this errback is called when something fails in processing a
            # message; it aborts the connection and gracefully shuts this
            # protocol down
            self._log.error(
                format='Error in message processing.',
                log_failure=fail
            )
            error_msg = make_message('status',
                                     {'error': 'Invalid message.'})
            self._send(error_msg)
            self._interface.finish_experiment_instance(exp_id)
            self.transport.loseConnection()

        self._current_d = Deferred() \
            .addCallback(validate_message) \
            .addCallback(callback) \
            .addErrback(errback)

    # noinspection PyArgumentList
    def _send(self, o: Any) -> None:
        self.transport.write(self._packer.pack(o))

    def dataReceived(self, data: bytes):
        self._unpacker.feed(data)
        for msg in self._unpacker:
            self._current_d.callback(msg)


class ExperimentRecordingServer(Factory):
    def __init__(self,
                 db_engine: Engine,
                 output_dir: Path,
                 record_chunk_size: int = 100,
                 records_filename: str = 'records.csv',
                 metadata_filename: str = 'metadata.json',
                 times_filename: str = 'times.json',
                 default_metadata: Mapping[str, Any] = {}):
        # TODO: document
        self._log = Logger()

        self._engine = db_engine
        self._interface = BufferedExperimentInterface(
            db_engine=self._engine,
            chunk_size=record_chunk_size,
            default_metadata=default_metadata)

        self._out_dir = output_dir.resolve()
        self._out_dir.mkdir(exist_ok=True, parents=True)

        def check_path(p: Path) -> Path:
            if p.exists():
                if p.is_dir():
                    raise FileExistsError(p)
                else:
                    self._log.warn(
                        format='{path} will be overwritten with new data.',
                        path=p
                    )
            return p.resolve()

        self._records_path = check_path(self._out_dir / records_filename)
        self._metadata_path = check_path(self._out_dir / metadata_filename)
        self._times_path = check_path(self._out_dir / times_filename)

        self._backlog_lc = None

    @property
    def records_path(self) -> Path:
        return self._records_path

    @property
    def metadata_path(self) -> Path:
        return self._metadata_path

    @property
    def times_path(self) -> Path:
        return self._times_path

    def buildProtocol(self, addr: IAddress) -> SingleExperimentServer:
        self._log.info('Incoming connection from {addr}.', addr=addr)
        return SingleExperimentServer(self._interface, addr)

    def startFactory(self):
        self._log.info(format='Starting.')

        def interface_backlog():
            # callback for logging the backlog on the DB thread
            chunks, records = self._interface.backlog
            chunk_sz = self._interface.chunk_size
            self._log.info(
                format='Approx. record backlog: {chunks} chunks '
                       '(@{chunk_size} records per chunk = {records})',
                chunks=chunks, chunk_size=chunk_sz, records=records
            )

        self._log.debug(format='Starting DB backlog looping call.')
        self._backlog_lc = LoopingCall(interface_backlog)
        self._backlog_lc.start(interval=5.0)  # FIXME magic number

    def stopFactory(self):
        self._log.warn(format='Shutting down.')

        if self._backlog_lc is not None:
            self._backlog_lc.stop()

        self._log.warn(format='Outputting results to {path}.',
                       path=self._out_dir)
        # get tables and dicts from the interface
        records = self._interface.records_as_dataframe()
        metadata = self._interface.metadata_as_dict()
        times = self._interface.experiment_times_as_dict()

        records.to_csv(self._records_path, index=True)

        with self._metadata_path.open('w') as fp:
            json.dump(metadata, fp, indent=4)

        with self._times_path.open('w') as fp:
            json.dump(times, fp, indent=4)

        self._interface.close()
