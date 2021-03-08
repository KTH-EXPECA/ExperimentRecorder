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
from typing import Any, Callable, Dict, Mapping

import msgpack
from twisted.internet.protocol import Protocol

from .messages import InvalidMessageError, MESSAGE_TYPES, make_message, \
    validate_message


class MessageProtocol(Protocol):
    """
    Handles the base conversion of msgpack messages into dictionaries of a
    specific form and the delegation of tasks to registered callbacks.
    """

    #: protocol version
    #: TODO: in the future, deal with version mismatch
    version_major = 1
    version_minor = 0

    def __init__(self):
        super(MessageProtocol, self).__init__()
        self._handlers: Dict[str, Callable] = {}
        self._unpacker = msgpack.Unpacker()

    def connectionMade(self):
        version_msg = make_message('version',
                                   {
                                       'major': self.version_major,
                                       'minor': self.version_minor
                                   })
        self._send(version_msg)

    def dataReceived(self, data: bytes) -> None:
        self._unpacker.feed(data)
        # TODO: log
        for msg_dict in self._unpacker:
            try:
                mtype, payload = validate_message(msg_dict)
                self._handlers.get(mtype, self._default_handler)(payload)
            except InvalidMessageError as e:
                reply = make_message('status', {
                    'success': False,
                    'error'  : 'Invalid message.'
                })
            except Exception as e:
                # if anything fails in the handler, we send a fail status msg
                reply = make_message('status', {
                    'success': False,
                    'error'  : 'Error while processing request.'
                })
            else:
                # if everything goes right, we send a success status msg
                reply = make_message('status', {'success': True})
            self._send(reply)

    def handler(self, msg_type: str):
        """
        Decorator to register a handler for a message type.

        Parameters
        ----------
        msg_type
            The message type handled by the wrapped function.

        Returns
        -------
        wrapper
            A decorator for registering a handler for the specified message
            type.
        """

        assert msg_type in MESSAGE_TYPES

        def wrapper(fn: Callable[..., None]) -> None:
            self._handlers[msg_type] = fn

        return wrapper

    def _default_handler(self, payload: Mapping[str, Any]) -> None:
        reply = make_message('status', {
            'success': False,
            'error'  : 'No registered handler for this message type.'
        })
        self._send(reply)

    def _send(self, o: Any) -> None:
        msgpack.pack(o, self.transport)
