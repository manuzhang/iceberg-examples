import pathlib
import re
import unittest


class FlinkSqlExampleSmokeTest(unittest.TestCase):
    def test_sql_pipeline_contains_iceberg_catalog(self) -> None:
        sql = pathlib.Path(__file__).with_name("sql_pipeline.sql").read_text()

        self.assertIn("CREATE CATALOG iceberg_catalog", sql)
        self.assertIn("'type'='iceberg'", sql)
        self.assertIn("WATERMARK FOR event_time", sql)
        self.assertRegex(sql, re.compile(r"INSERT\s+INTO\s+orders", re.IGNORECASE))
        self.assertRegex(sql, re.compile(r"SELECT\s+customer_id", re.IGNORECASE))


if __name__ == "__main__":
    unittest.main()
