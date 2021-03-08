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
from typing import Any, Callable, Dict, Mapping, Tuple

import msgpack
from twisted.internet.protocol import Factory, Protocol

from .messages import InvalidMessageError, MESSAGE_TYPES, make_message, \
    validate_message


# msgpack needs special code to pack/unpack int, float and bool types
# into strings
class MessagePacker(msgpack.Packer):
    @staticmethod
    def encode_vartype(obj: Any) -> Mapping[str, Any]:
        if isinstance(obj, type(int)):
            return {'__vartype__': 'int'}
        elif isinstance(obj, type(float)):
            return {'__vartype__': 'float'}
        elif isinstance(obj, type(bool)):
            return {'__vartype__': 'bool'}
        else:
            return obj

    def __init__(self, *args, **kwargs):
        kwargs['default'] = self.encode_vartype
        super(MessagePacker, self).__init__(*args, **kwargs)


class MessageUnpacker(msgpack.Unpacker):
    @staticmethod
    def decode_vartype(data: Any) -> Any:
        try:
            return eval(data['__vartype__'])
        except:
            return data

    def __init__(self, *args, **kwargs):
        kwargs['object_hook'] = self.decode_vartype
        super(MessageUnpacker, self).__init__(*args, **kwargs)


class HandlerCallback:
    def __init__(self, callback: Callable, unpack: bool = False):
        super(HandlerCallback, self).__init__()
        self._cb = callback
        self._unpack = unpack

    def __call__(self, payload: Mapping[str, Any]):
        if self._unpack:
            return self._cb(**payload)
        else:
            return self._cb(payload)

    def call(self, payload: Mapping):
        return self(payload)


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
        self._handlers: Dict[str, HandlerCallback] = {}

        self._unpacker = MessageUnpacker()
        self._packer = MessagePacker()

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
                self._handlers.get(mtype, self._default_handler).call(payload)
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

    def handler(self, msg_type: str, unpack: bool = False):
        """
        Decorator to register a handler for a message type.

        Parameters
        ----------
        msg_type
            The message type handled by the wrapped function.
        unpack
            Whether to unpack the message payload into keyword arguments when
            calling this handler.

        Returns
        -------
        wrapper
            A decorator for registering a handler for the specified message
            type.
        """

        assert msg_type in MESSAGE_TYPES

        def wrapper(fn: Callable[..., None]) -> None:
            self._handlers[msg_type] = HandlerCallback(fn, unpack=unpack)

        return wrapper

    def _default_handler_fn(self, payload: Mapping[str, Any]) -> None:
        reply = make_message('status', {
            'success': False,
            'error'  : 'No registered handler for this message type.'
        })
        self._send(reply)

    _default_handler = HandlerCallback(_default_handler_fn)

    # noinspection PyArgumentList
    def _send(self, o: Any) -> None:
        self.transport.write(self._packer.pack(o))


class _MsgProtocolFactory(Factory):
    def buildProtocol(self, addr: Tuple[str, int]) -> Protocol:
        return MessageProtocol()


msg_protocol_factory = _MsgProtocolFactory()
