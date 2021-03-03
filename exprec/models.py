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

from typing import Type, Union

import tables

VARTYPES = Union[Type[int], Type[float], Type[bool]]

_vartype_columns = {
    int  : tables.Int64Col,
    float: tables.Float64Col,
    bool : tables.BoolCol
}


class TimeStampTable(tables.IsDescription):
    record_time = tables.Time64Col()
    report_time = tables.Time64Col()


def make_variable_table(vartype: Union[Type[int], Type[float], Type[bool]]) \
        -> Type[TimeStampTable]:
    try:
        col = _vartype_columns[vartype]()
    except KeyError:
        raise RuntimeError(f'Unsupported variable type {vartype}.')

    # noinspection PyTypeChecker
    return type('VarTable', (TimeStampTable,), {'value': col})
