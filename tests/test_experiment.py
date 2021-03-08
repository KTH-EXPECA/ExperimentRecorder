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
from pathlib import Path
from typing import Any, NamedTuple, Type

import tables
from twisted.trial import unittest

from exprec import ExperimentWriter
from exprec.experiment import ExperimentElementError


class TestVariable(NamedTuple):
    name: str
    type: Type
    value: Any


class TestExperiments(unittest.TestCase):
    h5file_path = Path('/tmp/h5test.h5')
    exp_id = 'TestExperiment'
    exp_title = 'Test Experiment'

    exp_vars_valid = [
        TestVariable('a', int, 3),
        TestVariable('b', float, 3.9),
        TestVariable('c', bool, False)
    ]
    exp_vars_invalid = [
        TestVariable('d', str, 'Foo'),
        TestVariable('e', bytes, b'Bar'),
        TestVariable('f', object, object())
    ]

    sub_exp_id = 'SubExperiment'
    sub_exp_title = 'Sub Experiment'

    def setUp(self) -> None:
        self.assertFalse(self.h5file_path.exists())

        # open an experiment, check that the file exists too
        self.experiment = ExperimentWriter.create(
            file_path=self.h5file_path,
            exp_id=self.exp_id,
            exp_title=self.exp_title,
            variables={v.name: v.type for v in self.exp_vars_valid}
        )

        self.assertTrue(self.h5file_path.exists())

    def tearDown(self) -> None:
        # flush, close and delete the file
        self.experiment.flush()
        self.experiment.close()
        self.assertFalse(self.experiment.h5file.isopen)
        self.h5file_path.unlink(missing_ok=False)

    def test_experiment_creation(self):
        # check that the experiment was created correctly
        file = self.experiment.h5file

        # check that the experiment group was created correctly, has the
        # right type, and the correct title
        try:
            g = file.get_node(file.root, self.exp_id)
            self.assertIsInstance(g, tables.Group)
            self.assertEqual(g._v_attrs.TITLE, self.exp_title)
        except tables.NoSuchNodeError:
            self.fail('Could not find experiment group in HDF5 File.')

    def test_sub_experiment_creation(self):
        # with sub_experiments we can use the context manager mode

        def make_sub_exp():
            return self.experiment.make_sub_experiment(
                sub_exp_id=self.sub_exp_id,
                variables={v.name: v.type for v in self.exp_vars_valid},
                sub_exp_title=self.sub_exp_title
            )

        with make_sub_exp() as sub_exp:
            # file should be the same
            self.assertEqual(self.experiment.h5file, sub_exp.h5file)
            file = sub_exp.h5file

            # check that the experiment group was created correctly, has the
            # right type, and the correct title
            try:
                g = file.get_node(self.experiment._group, self.sub_exp_id)
                self.assertIsInstance(g, tables.Group)
                self.assertEqual(g._v_attrs.TITLE, self.sub_exp_title)
            except tables.NoSuchNodeError:
                self.fail('Could not find sub-experiment group in HDF5 File.')

            # check that we can get the same experiment from the top level
            # experiment by looking it up
            with self.experiment.get_sub_experiment(self.sub_exp_id) as \
                    same_sub_exp:
                self.assertEqual(same_sub_exp, sub_exp)

        # trying to recreate a sub-experiment should fail
        self.assertRaises(ExperimentElementError, make_sub_exp)

    def test_write_variables(self):
        # check that values are actually stored correctly
        file = self.experiment.h5file
        for var in self.exp_vars_valid:
            self.experiment.record_variable(var.name, var.value, 0.0)

            # can't store mismatching types
            # boolean columns seem to coerce everything to
            # True/False, so they don't raise any TypeErrors
            if var.type != bool:
                self.assertRaises(TypeError,
                                  self.experiment.record_variable,
                                  var.name, object(), 1)

            # check the value
            tbl = file.get_node(self.experiment._group, var.name)
            self.assertIsInstance(tbl, tables.Table)
            tbl.flush()  # needed since we're gonna read it

            vals = [row['value'] for row in tbl.iterrows()]

            self.assertEqual(var.value, vals[0])

        # can't record variables which weren't registered
        for var in self.exp_vars_invalid:
            self.assertRaises(
                ExperimentElementError,
                lambda: self.experiment.record_variable(var.name, var.value, 0)
            )

    def test_dont_overwrite_file(self):
        # trying to create an experiment on an already existing path should fail
        with self.assertRaises(FileExistsError):
            ExperimentWriter.create(
                file_path=self.h5file_path,
                exp_id=self.exp_id,
                exp_title=self.exp_title,
                variables={}
            )

    def test_invalid_variables(self):
        # trying to create an experiment with invalid variable types should fail
        # and the file should be cleaned up
        fpath = Path('/tmp/should_not_exist.h5')
        with self.assertRaises(ExperimentElementError):
            ExperimentWriter.create(
                file_path=fpath,
                exp_id='invalid',
                exp_title='invalid variable types',
                variables={var.name: var.type for var in self.exp_vars_invalid}
            )

        self.assertFalse(fpath.exists())

    def test_register_sub_exp_with_name_conflict(self):
        # trying to register an experiment with the same name as an existing
        # variable should fail
        for var in self.exp_vars_valid:
            self.assertRaises(ExperimentElementError,
                              self.experiment.make_sub_experiment,
                              sub_exp_id=var.name,
                              variables={})

        # registering a valid sub-experiment twice should fail the second time
        self.experiment.make_sub_experiment(sub_exp_id=self.sub_exp_id,
                                            variables={})
        self.assertRaises(ExperimentElementError,
                          self.experiment.make_sub_experiment,
                          sub_exp_id=self.sub_exp_id,
                          variables={})
