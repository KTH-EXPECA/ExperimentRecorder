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
