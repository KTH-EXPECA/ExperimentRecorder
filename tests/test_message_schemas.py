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

from twisted.trial import unittest
from typing import Callable

from exprec.messages import InvalidMessageError, validate_message

valid_payloads = {
    'version': {
        'major': 13,
        'minor': 12
    },
    'init'   : {
        'experiment_id': 'test_experiment',
        'variables'    : {'a': int, 'b': float, 'c': bool}
    },
    'finish' : {
        'experiment_id': 'test_experiment'
    },
    'status' : {
        'success': True,
        'info'   : 'Success status!'
    },
    'record' : {
        'experiment_id': 'test_experiment',
        'variables'    : {
            'a': {'value': 1927, 'timestamp': 13.37},
            'b': {'value': 1.21, 'timestamp': 13.37},
            'c': {'value': False, 'timestamp': 13.37}
        }
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
