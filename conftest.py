import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    "creates a spark session"
    return get_spark_session("LOCAL")

@pytest.fixture
def spark():
    "creates a spark session with a yield"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()   

@pytest.fixture
def expected_results(spark): 
    "matching the expected count"
    results_schema ="state string,count int"
    return spark.read \
    .format("csv") \
    .schema(results_schema) \
    .load("test_result/expected_count.csv")
    


