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
from __future__ import annotations

import datetime
import uuid
from typing import Callable

from twisted.trial import unittest

from exprec.messages import InvalidMessageError, validate_message

valid_payloads = {
    'version' : {
        'major': 13,
        'minor': 12
    },
    'status'  : {
        'success': True,
        'info'   : 'Success status'
    },
    'record'  : {
        'timestamp': datetime.datetime.now(),  # let the protocol handle this
        'variables': {'a': 666}
    },
    'welcome' : {
        'instance_id': uuid.uuid4()  # let the protocol handle this
    },
    'metadata': {
        'meta': 'data',
        'foo' : 'bar'
    }
}


class DynamicTestsMeta(type):
    def __init__(cls, *args, **kwargs):
        super(DynamicTestsMeta, cls).__init__(*args, **kwargs)

        def make_test(msg_type: str) -> Callable:
            def _test(self: TestSchemas):
                # test that the correct message passes validation
                validate_message(
                    {
                        'type'   : msg_type,
                        'payload': valid_payloads[msg_type]
                    }
                )

                # test that none of the others pass
                for mtype, payload in valid_payloads.items():
                    if mtype != msg_type:
                        self.assertRaises(
                            InvalidMessageError,
                            validate_message,
                            {
                                'type'   : msg_type,
                                'payload': payload
                            }
                        )

            return _test

        for msg_t in valid_payloads.keys():
            setattr(cls, f'test_{msg_t}', make_test(msg_t))


class TestSchemas(unittest.TestCase, metaclass=DynamicTestsMeta):
    pass
