from lib.utils import get_spark_session
from lib.ConfigReader import get_app_config
from lib.DataReader import read_customers

spark=get_spark_session('LOCAL')

def get_aggergate_state(spark):
    customers_df=read_customers(spark,'LOCAL')
    state_aggregate_df=customers_df.groupBy("state")
    state_aggregate_df.count().write.format("csv").mode("overwrite").save("data/test_result/state_aggregate.csv")
    

