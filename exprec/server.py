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
from typing import Any, Mapping

import msgpack
from twisted.internet.protocol import Protocol

from exprec import ExperimentWriter


class ExperimentLoggingServerProtocol(Protocol):
    #: protocol version
    #: TODO: in the future, deal with version mismatch
    version_major = 1
    version_minor = 0

    def __init__(self, top_lvl_exp: ExperimentWriter):
        super(ExperimentLoggingServerProtocol, self).__init__()
        self._unpacker = msgpack.Unpacker()
        self._top_lvl_experiment = top_lvl_exp
        self._exp = None

        self._msg_type_handlers = {
            'record': self._handle_record_msg,
            'finish': self._handle_finish_msg,
        }

    def connectionMade(self) -> None:
        # send version information
        msgpack.dump({
            'version': {
                'major': self.version_major,
                'minor': self.version_minor
            }
        }, self.transport)

    def dataReceived(self, data: bytes) -> None:
        self._unpacker.feed(data)
        for msg_dict in self._unpacker:
            if self._exp is None:
                # still waiting for initialization of sub exp
                # TODO
                pass
            else:
                try:
                    self._msg_type_handlers.get(
                        msg_dict['type'],
                        self._handle_invalid_msg
                    )(payload=msg_dict['payload'])
                except KeyError:
                    # TODO: handle malformed message
                    pass

    def _handle_record_msg(self, payload: Mapping[str, Any]) -> None:
        pass

    def _handle_finish_msg(self, payload: Mapping[str, Any]) -> None:
        pass

    def _handle_invalid_msg(self, msg: Mapping[str, Any]) -> None:
        pass
