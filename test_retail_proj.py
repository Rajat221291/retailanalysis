import pytest
from lib.utils import get_spark_session
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config

@pytest.mark.transformation
@pytest.mark.regression
def test_read_customers_df(spark):
    customers_count=read_customers(spark,'LOCAL').count()
    assert customers_count==12435
    

def test_read_orders_df(spark):
    orders_count=read_orders(spark,'LOCAL').count()
    assert orders_count== 68884
    
def test_filter_orders_df(spark):
    orders_df=read_orders(spark,'LOCAL')
    orders_count=filter_closed_orders(orders_df).count()
    assert orders_count== 7556

def test_read_app_config():
    config=get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"
    
    
@pytest.mark.transformation
def test_count_orders_state(spark,expected_result):
    customers_df=read_customers(spark,'LOCAL')
    state_aggregate_df=count_orders_state(customers_df)
    assert state_aggregate_df.collect() == expected_result.collect()

@pytest.mark.parametrize(
    "status,count",
    [("CLOSED",7556),
     ("PENDING_PAYMENT",15030),
     ("COMPLETE",22900)]
)
    


def test_check_closed_count(spark,status,count):
    orders_df=read_orders(spark,'LOCAL')
    orders_count=filter_orders_generic(orders_df,status).count()
    assert orders_count== count