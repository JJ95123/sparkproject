import pytest
from lib.Utils import get_spark_session
from lib import DataManipulation,DataReader,ConfigReader
import conftest


@pytest.fixture
def spark():
    return get_spark_session("LOCAL")

def test_read_customers_df(spark):
    customers_count = DataReader.read_customers(spark,"LOCAL").count()
    print("customers_count",customers_count)
    assert customers_count == 12436
    print("latest functioncalled")
    

def test_read_orders_df(spark):
    orders_count = DataReader.read_orders(spark,"LOCAL").count()
    print("orders_count",orders_count)
    assert orders_count == 68885

@pytest.mark.parametrize(
        "entry1,count",
        [("CLOSED",7556),
         ("PENDING_PAYMENT",15030),
         ("COMPLETE",22899)]

)
@pytest.mark.latest()
def count_filter_orders(spark,entry1,count):
    orders_df = DataReader.read_orders(spark,"LOCAL")
    filtered_orders_count = DataManipulation.filter_orders_generic(orders_df,entry1).count()
    assert filtered_orders_count == count


def  read_app_config():
    configs = ConfigReader.get_app_config("LOCAL") 
    assert configs["customers.file.path"]  == "data/customers.csv" 


def test_match_expected_count(spark,expected_results):
    customers_df = DataReader.read_customers(spark,"LOCAL")
    actual_count_df = DataManipulation.count_orders_state(customers_df)
    assert actual_count_df.collect() == expected_results.collect()

    
        



