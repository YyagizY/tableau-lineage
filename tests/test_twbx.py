"""Stdlib-only tests for the .twbx lineage extractor."""

import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tableau_fetch.twbx import load_twbx, to_json_payload  # noqa: E402


def _write_twbx(tmp: Path, xml: str, name: str = "wb.twbx") -> Path:
    path = tmp / name
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("wb.twb", xml)
    return path


_SIMPLE_XML = """<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    <datasource name='ds1' caption='sku_breakdown'>
      <connection class='databricks-spark-sql' dbname='/invent-fivebelow-datastore/algorithm/dcrpl_dfu_sku_breakdown' />
      <column name='[product_code]' datatype='string' caption='Product Code' />
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='Sheet 1'>
      <table><view><datasources><datasource name='ds1' caption='sku_breakdown' /></datasources></view></table>
    </worksheet>
  </worksheets>
</workbook>
"""


_CALC_XML = """<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    <datasource name='ds1' caption='sku_breakdown'>
      <connection class='databricks-spark-sql' dbname='/p/a/b' />
      <column name='[sales]' datatype='real' caption='Sales' />
      <column name='[Calculation_1]' datatype='real' caption='Avg Sales'>
        <calculation class='tableau' formula='SUM([sales]) / COUNT([sales])' />
      </column>
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='S1'>
      <table><view><datasources><datasource name='ds1' caption='sku_breakdown' /></datasources></view></table>
    </worksheet>
  </worksheets>
</workbook>
"""


_MULTI_XML = """<?xml version='1.0' encoding='utf-8'?>
<workbook>
  <datasources>
    <datasource name='ds1' caption='sku_breakdown'>
      <connection class='databricks-spark-sql' dbname='/p/a/t1' />
      <column name='[x]' datatype='integer' caption='X' />
    </datasource>
    <datasource name='ds2' caption='store_dim'>
      <connection class='databricks-spark-sql' dbname='/p/b/t2' />
      <column name='[y]' datatype='string' caption='Y' />
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='Sheet A'>
      <table><view><datasources><datasource name='ds1' caption='sku_breakdown' /></datasources></view></table>
    </worksheet>
    <worksheet name='Sheet B'>
      <table><view><datasources><datasource name='ds2' caption='store_dim' /></datasources></view></table>
    </worksheet>
  </worksheets>
</workbook>
"""


class TwbxLineageTests(unittest.TestCase):
    def test_plain_dimension_field(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_twbx(Path(d), _SIMPLE_XML)
            sheets = load_twbx(path)

            self.assertEqual(len(sheets), 1)
            s = sheets[0]
            self.assertEqual(s.sheet, "Sheet 1")
            self.assertEqual(s.datasource.tableau_datasource_name, "sku_breakdown")
            self.assertEqual(
                s.datasource.delta_table,
                "/invent-fivebelow-datastore/algorithm/dcrpl_dfu_sku_breakdown",
            )

            self.assertEqual(len(s.fields), 1)
            f = s.fields[0]
            self.assertEqual(f.displayed_name, "Product Code")
            self.assertEqual(f.original_column, "product_code")
            self.assertEqual(f.data_type, "STRING")
            self.assertFalse(f.is_calculated)
            self.assertIsNone(f.formula)

            payload = to_json_payload(sheets)
            self.assertIsInstance(payload, dict)
            self.assertEqual(payload["workbook"], "wb")

    def test_calculated_field(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_twbx(Path(d), _CALC_XML)
            sheets = load_twbx(path)

            calc_fields = [f for f in sheets[0].fields if f.is_calculated]
            self.assertEqual(len(calc_fields), 1)
            calc = calc_fields[0]

            self.assertEqual(calc.displayed_name, "Avg Sales")
            self.assertIsNone(calc.original_column)
            self.assertEqual(calc.formula, "SUM([sales]) / COUNT([sales])")
            self.assertEqual(calc.data_type, "REAL")

    def test_multiple_sheets_with_distinct_datasources(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_twbx(Path(d), _MULTI_XML)
            sheets = load_twbx(path)

            self.assertEqual(len(sheets), 2)
            by_name = {s.sheet: s for s in sheets}
            self.assertEqual(set(by_name), {"Sheet A", "Sheet B"})
            self.assertEqual(by_name["Sheet A"].datasource.delta_table, "/p/a/t1")
            self.assertEqual(by_name["Sheet B"].datasource.delta_table, "/p/b/t2")

            payload = to_json_payload(sheets)
            self.assertIsInstance(payload, list)
            self.assertEqual(len(payload), 2)


if __name__ == "__main__":
    unittest.main()
