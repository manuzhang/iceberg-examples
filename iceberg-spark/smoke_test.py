import ast
import pathlib
import unittest


def top_level_functions(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text())
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


class SparkExamplesSmokeTest(unittest.TestCase):
    def test_batch_pipeline_symbols_exist(self) -> None:
        functions = top_level_functions(pathlib.Path(__file__).with_name("batch_pipeline.py"))
        self.assertTrue({"orders", "validated_orders", "daily_sales", "customer_ltv"} <= functions)

    def test_streaming_pipeline_symbols_exist(self) -> None:
        functions = top_level_functions(pathlib.Path(__file__).with_name("streaming_pipeline.py"))
        self.assertTrue(
            {
                "sensor_events",
                "temperature_readings",
                "humidity_readings",
                "sensor_hourly_stats",
            }
            <= functions
        )


if __name__ == "__main__":
    unittest.main()
