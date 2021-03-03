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

import os
from unittest import TestCase

import tables

from exprec.experiment import ExperimentWriter, _ExperimentMetadata


class TestExperimentWriter(TestCase):
    FNAME = '/tmp/test.h5'
    EXPNAME = 'TestExperiment'
    EXPTITLE = 'Test Experiment Title'
    SUBEXPNAME = 'TestSubExperiment'
    SUBEXPTITLE = 'Test SubExperiment Title'

    EXPVARS = {
        'float': float,
        'int'  : int,
        'bool' : bool
    }

    EXPVARSUPD = {
        'float': 13.12,
        'int'  : 666,
        'bool' : False
    }

    def setUp(self) -> None:
        self.h5file = tables.open_file(self.FNAME, mode='a')
        self.exp_writer = ExperimentWriter(name=self.EXPNAME,
                                           h5file=self.h5file,
                                           title=self.EXPTITLE)

        self._exp_path = f'{_ExperimentMetadata.EXPERIMENT_GROUP}/' \
                         f'{self.EXPNAME}'
        self.assertIn(self._exp_path, self.h5file)

    def tearDown(self) -> None:
        self.h5file.close()
        os.remove(self.FNAME)

    # def test_flush(self):
    #     self.fail()
    #
    # def test_experiment_id(self):
    #     self.fail()

    def test_sub_experiment(self):
        self.exp_writer.sub_experiment(name=self.SUBEXPNAME,
                                       title=self.SUBEXPTITLE)
        subexp_path = f'{self._exp_path}/{self.SUBEXPNAME}'

        self.assertIn(subexp_path, self.h5file)
        group = self.h5file.get_node(subexp_path)

        self.assertIsInstance(group, tables.Group)

    def test_register_variable(self):
        for name, vartype in self.EXPVARS.items():
            self.exp_writer.register_variable(name, vartype)
            table_path = f'{self._exp_path}/{name}'

            self.assertIn(table_path, self.h5file)

            node = self.h5file.get_node(table_path)
            self.assertIsInstance(node, tables.Table)

    # def test_update_variable(self):
    #     self.fail()
