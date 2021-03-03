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

from unittest import TestCase

import tables

from exprec import models


class Test(TestCase):
    def setUp(self) -> None:
        self._vartable_types = models._vartype_columns

    def test_make_variable_table(self):
        for t, c in self._vartable_types.items():
            tbl = models.make_variable_table(t)

            self.assertIsInstance(tbl(), tables.IsDescription)
            self.assertIn('value', tbl.columns)
            self.assertIsInstance(tbl.columns['value'], c)
