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

import uuid

from twisted.internet.address import IPv4Address, IPv6Address, UNIXAddress
from twisted.internet.defer import Deferred
from twisted.internet.interfaces import IAddress
from twisted.internet.protocol import Factory, Protocol
from twisted.logger import Logger
from twisted.python.failure import Failure

from .exp_interface import BufferedExperimentInterface
# msgpack needs special code to pack/unpack datetimes and uuids
from ..common.messages import make_message, \
    validate_message
from ..common.packing import MessagePacker, MessageUnpacker
from ..common.protocol import *


# ---

class MessageProtocol(Protocol):
    #: protocol version
    #: TODO: in the future, deal with version mismatch
    version_major = 1
    version_minor = 0

    def __init__(self,
                 interface: BufferedExperimentInterface,
                 addr: IAddress):
        # TODO: document

        super(MessageProtocol, self).__init__()
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
                welcome_msg = make_message(
                    msg_type='welcome',
                    payload={'instance_id': experiment_id}
                )
                self._send(welcome_msg)

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


class MessageProtoFactory(Factory):
    def __init__(self, interface: BufferedExperimentInterface):
        self._interface = interface

    def buildProtocol(self, addr: IAddress) -> MessageProtocol:
        return MessageProtocol(self._interface, addr)
