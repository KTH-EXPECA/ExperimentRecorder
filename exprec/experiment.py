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
from os import PathLike
from pathlib import Path
from typing import Mapping, Type, Union

import tables

VarType = Union[Type[int], Type[float], Type[bool]]
VarValue = Union[int, float, bool]

_vartype_columns = {
    int  : tables.Int64Col,
    float: tables.Float64Col,
    bool : tables.BoolCol
}


class ExperimentWriter:
    def __init__(self,
                 h5file: tables.File,
                 parent_group: tables.Group,
                 exp_id: str,
                 exp_title: str,
                 variables: Mapping[str, VarType]):
        super(ExperimentWriter, self).__init__()

        self._id = exp_id
        self._title = exp_title

        self._file = h5file
        self._group = h5file.create_group(parent_group, exp_id, title=exp_title)

        # metadata
        self._group._v_attrs.created = datetime.datetime.now().isoformat()
        self._group._v_attrs.finished = 'unfinished'

        self._var_tables = {}
        for var_name, var_type in variables.items():
            # TODO handle wrong type
            tbl = h5file.create_table(
                self._group, var_name,
                description={
                    'record_time'    : tables.Time64Col(),
                    'experiment_time': tables.Time64Col(),
                    'value'          : _vartype_columns[var_type]()
                })
            self._var_tables[var_name] = tbl

    @property
    def get_id(self) -> str:
        return self._id

    @property
    def get_title(self) -> str:
        return self._title

    @staticmethod
    def create(file_path: PathLike,
               exp_id: str,
               variables: Mapping[str, VarType],
               exp_title: str = '') -> ExperimentWriter:
        """
        Creates the HDF file and initializes an experiment on it.

        TODO

        :param file_path:
        :param exp_id:
        :param variables:
        :param exp_title:
        :return:
        """

        # make sure parent folders exist
        file_path = Path(file_path)
        file_path.parent.mkdir(exist_ok=True, parents=True)
        h5 = tables.open_file(str(file_path), mode='a',
                              title='Experiment Data File')

        return ExperimentWriter(h5, h5.root, exp_id, exp_title, variables)

    def make_sub_experiment(self,
                            sub_exp_id: str,
                            variables: Mapping[str, VarType],
                            sub_exp_title: str = '') -> ExperimentWriter:
        return ExperimentWriter(self._file,
                                self._group,
                                sub_exp_id,
                                sub_exp_title,
                                variables)

    def record_variable(self,
                        name: str,
                        value: VarValue,
                        timestamp: float) -> None:
        pass

    def close(self) -> None:
        """
        Flushes and closes the underlying file. Timestamp ending.
        :return:
        """

    def __enter__(self) -> ExperimentWriter:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
