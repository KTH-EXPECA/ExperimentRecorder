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

import abc
import datetime
from os import PathLike
from pathlib import Path
from typing import Mapping, Type, Union

import tables

__all__ = ['VarType', 'VarValue', 'ExperimentWriter']

#: valid types for experiment variables
VarType = Union[Type[int], Type[float], Type[bool]]

#: valid values for experiment variables
VarValue = Union[int, float, bool]

_vartype_columns = {
    int  : tables.Int64Col,
    float: tables.Float64Col,
    bool : tables.BoolCol
}


class ExperimentElementError(Exception):
    pass


class UnsupportedVariableType(ExperimentElementError):
    def __init__(self, t: Type):
        super(UnsupportedVariableType, self).__init__(t)


class ExperimentWriter(abc.ABC):
    """
    Class for writing experiment data to an HDF5 file in a structured manner.
    This class sets up a recursive structure consisting of experiments which
    contain variable tables and other sub-experiments under it.

    This class is an interface and doesn't have a valid conventional
    constructor. Instead, users are directed to the ExperimentWriter.create()
    static method to create a valid instance of this class.
    """

    @property
    @abc.abstractmethod
    def h5file(self) -> tables.File:
        """
        Returns
        -------
        tables.File
           The underlying HDF5 file associated with this ExperimentWriter.
        """
        pass

    @property
    @abc.abstractmethod
    def get_id(self) -> str:
        """

        Returns
        -------
        experiment_id
            The unique ID of this experiment, used to identify it in the HDF5
            file data structure.
        """
        pass

    @property
    @abc.abstractmethod
    def get_title(self) -> str:
        """

        Returns
        -------
        experiment_title
            The title, or one-line description of this experiment.
        """
        pass

    @abc.abstractmethod
    def make_sub_experiment(self,
                            sub_exp_id: str,
                            variables: Mapping[str, VarType],
                            sub_exp_title: str = '') -> ExperimentWriter:
        """
        Create a sub-experiment under the current experiment.

        Parameters
        ----------
        sub_exp_id
            Unique ID of the sub-experiment.
        variables
            A mapping, variable_name -> variable_type, indicating the desired
            variables to store under the sub-experiment.
        sub_exp_title
            The title, or one-line description of this sub-experiment.

        Returns
        -------
        sub_exp_writer
            An ExperimentWriter instance.
        """
        pass

    @abc.abstractmethod
    def get_sub_experiment(self, sub_exp_id: str) -> ExperimentWriter:
        """
        Look up a sub-experiment by ID.

        Parameters
        ----------
        sub_exp_id
            Unique ID of the sub-experiment.

        Returns
        -------
        sub_exp_writer
            An ExperimentWriter instance.
        """
        pass

    @abc.abstractmethod
    def record_variable(self,
                        name: str,
                        value: VarValue,
                        timestamp: float) -> None:
        """
        Record a new value for a registered variable.

        Parameters
        ----------
        name
            Name of the variable to update.
        value
            New value to record.
        timestamp
            UNIX timestamp for the new value.

        Returns
        -------

        """
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """
        Closes this experiment and all associated sub-experiments.

        Returns
        -------

        """
        pass

    @abc.abstractmethod
    def flush(self) -> None:
        """
        Flushes this experiment and all associated sub-experiments to disk.

        Returns
        -------

        """
        pass

    def __enter__(self) -> ExperimentWriter:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def create(file_path: PathLike,
               exp_id: str,
               variables: Mapping[str, VarType],
               exp_title: str = '') -> ExperimentWriter:
        """
        Initializes an HDF5 file and constructs an experiment using it as the
        underlying structure.

        Parameters
        ----------
        file_path
            Path to the new HDF5 file. Should not exist, otherwise this
            a FileExistsError will be raised.
        exp_id
            Unique ID for this experiment.
        variables
            A mapping, variable_name -> variable_type, indicating the desired
            variables to store under the experiment.
        exp_title
            The title, or one-line description of this experiment.

        Returns
        -------
        exp_writer
            An ExperimentWriter instance.
        """

        # make sure parent folders exist, but file should be new
        file_path = Path(file_path)
        if file_path.exists():
            # file exists, don't delete but raise an error
            raise FileExistsError(file_path)

        file_path.parent.mkdir(exist_ok=True, parents=True)
        h5 = tables.open_file(str(file_path), mode='a',
                              title='Experiment Data File')
        try:
            return _TopLevelExperimentWriter(h5,
                                             h5.root,
                                             exp_id,
                                             exp_title,
                                             variables)
        except ExperimentElementError:
            # error while creating experiment.
            # delete the file to avoid leaving junk around
            # then re-raise error
            h5.close()
            file_path.unlink()
            raise


# noinspection PyProtectedMember
class _ExperimentWriter(ExperimentWriter):
    # TODO: descriptive exceptions?
    def __init__(self,
                 h5file: tables.File,
                 parent_group: tables.Group,
                 exp_id: str,
                 exp_title: str,
                 variables: Mapping[str, VarType]):
        super(_ExperimentWriter, self).__init__()

        self._id = exp_id
        self._title = exp_title

        self._file = h5file
        try:
            self._group = h5file.create_group(parent_group, exp_id,
                                              title=exp_title)
        except tables.NodeError:
            try:
                node = h5file.get_node(parent_group, exp_id)
                path = node._v_pathname
                if isinstance(node, tables.Group):
                    raise ExperimentElementError('Experiment already exists at'
                                                 f'{path} in file {h5file}.')
                elif isinstance(node, tables.Table):
                    raise ExperimentElementError('Name conflict: variable '
                                                 'table already exists at '
                                                 f'{path} in file {h5file}')
                else:
                    raise ExperimentElementError(f'Conflict at {path} '
                                                 f'in file {h5file}')
            except tables.NoSuchNodeError as e:
                raise ExperimentElementError() from e

        # metadata
        self._group._v_attrs.created = datetime.datetime.now().isoformat()
        self._group._v_attrs.finished = 'unfinished'

        self._var_tables = dict()
        for var_name, var_type in variables.items():
            try:
                tbl = h5file.create_table(
                    self._group, var_name,
                    description={
                        'record_time'    : tables.Time64Col(),
                        'experiment_time': tables.Time64Col(),
                        'value'          : _vartype_columns[var_type]()
                    })
                self._var_tables[var_name] = tbl
            except KeyError:
                raise UnsupportedVariableType(var_type)

        self._sub_experiments = dict()

    @property
    def h5file(self) -> tables.File:
        return self._file

    @property
    def get_id(self) -> str:
        return self._id

    @property
    def get_title(self) -> str:
        return self._title

    def make_sub_experiment(self,
                            sub_exp_id: str,
                            variables: Mapping[str, VarType],
                            sub_exp_title: str = '') -> ExperimentWriter:

        if sub_exp_id in self._sub_experiments:
            raise ExperimentElementError(f'Sub-experiment {sub_exp_id} '
                                         f'already exists under '
                                         f'{self._group._v_pathname}.')
        elif sub_exp_id in self._var_tables:
            raise ExperimentElementError('Naming conflict: variable '
                                         f'table {sub_exp_id}already exists '
                                         f'under {self._group._v_pathname}')

        sub_exp = _ExperimentWriter(self._file,
                                    self._group,
                                    sub_exp_id,
                                    sub_exp_title,
                                    variables)
        self._sub_experiments[sub_exp_id] = sub_exp
        return sub_exp

    def get_sub_experiment(self, sub_exp_id: str) -> ExperimentWriter:
        return self._sub_experiments[sub_exp_id]

    def record_variable(self,
                        name: str,
                        value: VarValue,
                        timestamp: float) -> None:
        try:
            row = self._var_tables[name].row
            row['value'] = value
            row['experiment_time'] = timestamp
            row['record_time'] = datetime.datetime.now().timestamp()
            row.append()
        except KeyError:
            raise ExperimentElementError(f'No such variable {name}.')

    def flush(self) -> None:
        # flush everything
        for _, tbl in self._var_tables.items():
            tbl.flush()

        for _, sexp in self._sub_experiments.items():
            sexp.flush()

    def close(self) -> None:
        self.flush()

        # close all sub-experiments
        for _, sub_exp in self._sub_experiments.items():
            sub_exp.close()

        # timestamp our end time
        # TODO: boolean to indicate we've finished?
        self._group._v_attrs.finished = datetime.datetime.now().isoformat()


class _TopLevelExperimentWriter(_ExperimentWriter):
    def close(self) -> None:
        super(_TopLevelExperimentWriter, self).close()

        # top level experiment also closes the file
        self._file.flush()
        self._file.close()
