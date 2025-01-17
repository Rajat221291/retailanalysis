import pytest
from lib.utils import get_spark_session

@pytest.fixture
def spark():
    spark = get_spark_session('LOCAL')
    yield spark
    spark.stop()
    
@pytest.fixture
def expected_result(spark):
    "gives the expected results"
    results_schema="state string, count int"
    return spark.read.format("csv").schema(results_schema).load("data/test_result/state_aggregate.csv")