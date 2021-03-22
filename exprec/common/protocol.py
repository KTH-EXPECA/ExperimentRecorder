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
import functools
from typing import Any, Callable, Collection, Tuple

from .messages import ValidMessage


class ProtocolException(Exception):
    pass


class UnexpectedMessageException(ProtocolException):
    def __init__(self, expected_mtypes: Collection[str],
                 actual_mtype: str):
        super(UnexpectedMessageException, self).__init__(
            f'Expected one of \'{expected_mtypes}\', got \'{actual_mtype}\'.'
        )


class IncompatibleVersionException(ProtocolException):
    def __init__(self,
                 client_version: Tuple[int, int],
                 server_version: Tuple[int, int]):
        super(IncompatibleVersionException, self).__init__(
            f'Incompatible protocol versions. '
            f'Server v.{server_version[0]}.{server_version[1]}, '
            f'Client v.{client_version[0]}.{client_version[1]}.'
        )


def check_message_type(*expected_types: str) -> Callable:
    def _decorator(fn: Callable[[ValidMessage], Any]) -> Callable:
        @functools.wraps(fn)
        def _wrapper(*args):
            # hack to deal nicely with both bound and unbound methods.
            # the message will always be the last argument, since, if self is
            # present, it always goes first.
            msg = args[-1]
            if msg.mtype not in expected_types:
                raise UnexpectedMessageException(expected_mtypes=expected_types,
                                                 actual_mtype=msg.mtype)
            return fn(*args)

        return _wrapper

    return _decorator
