from __future__ import annotations

import abc
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Union

import tables
import tables.exceptions

from .models import VARTYPES, make_variable_table


class _ExperimentMetadata(abc.ABC):
    # TODO: make a reader?

    EXPERIMENT_GROUP = '/experiments'

    def __init__(self, name: str):
        self._name = name
        self._created = datetime.now()

    @property
    def name(self) -> str:
        return self._name

    @property
    def created(self) -> datetime:
        return self._created


class ExperimentVariableWriter(_ExperimentMetadata):
    def __init__(self,
                 name: str,
                 vartype: VARTYPES,
                 h5file: tables.File,
                 title: str = '',
                 parent: Union[str, tables.Group] = '/'):
        super(ExperimentVariableWriter, self).__init__(name)
        self._vartype = vartype

        self._tbl = h5file.create_table(
            where=parent,
            name=name,
            description=make_variable_table(vartype),
            title=title
        )

        # TODO how often to flush?

    def record_value(self, value: Any, exp_timestamp: float) -> None:
        row = self._tbl.row
        row['record_time'] = datetime.now().timestamp()
        row['report_time'] = exp_timestamp
        row['value'] = value

        row.append()

        # TODO: flush?

    def flush_to_disk(self) -> None:
        self._tbl.flush()


class ExperimentWriter(_ExperimentMetadata):
    def __init__(self,
                 name: str,
                 h5file: tables.File,
                 title: str = '',
                 parent: Union[tables.Group, str] =
                 _ExperimentMetadata.EXPERIMENT_GROUP):
        super(ExperimentWriter, self).__init__(name)
        self._file = h5file
        self._title = title
        self._exp_id = uuid.uuid4()  # for unique lookup
        self._mono_start_t = time.monotonic()

        try:
            self._group = h5file.get_node(parent, name=name)
        except tables.exceptions.NoSuchNodeError:
            self._group = h5file.create_group(parent, name=name,
                                              title=title, createparents=True)

        self._variables: Dict[str, ExperimentVariableWriter] = {}
        self._sub_experiments: Dict[uuid.UUID, ExperimentWriter] = {}

        # set metadata
        self._group._v_attrs.created_at = self.created.isoformat()

    def flush(self) -> None:
        for _, v in self._variables.items():
            v.flush_to_disk()

        for _, v in self._sub_experiments.items():
            _, v.flush()

    @property
    def experiment_id(self) -> uuid.uuid4():
        return self._exp_id

    def sub_experiment(self, name: str, title: str = '') -> ExperimentWriter:
        sub_exp = ExperimentWriter(name=name,
                                   h5file=self._file,
                                   title=title,
                                   parent=self._group)
        self._sub_experiments[sub_exp.experiment_id] = sub_exp
        return sub_exp

    def register_variable(self, name: str, vartype: VARTYPES) -> None:
        pass

    def update_variable(self,
                        name: str,
                        value: Any,
                        exp_timestamp: Optional[float] = None) -> None:
        pass
