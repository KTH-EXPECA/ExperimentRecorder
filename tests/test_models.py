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
