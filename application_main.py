# Main wrapper for application
import sys
from lib import DataManipulation,DataReader,utils
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__== '__main__':
    if len(sys.argv) <2 :
        print ("please enter the environment name")
        sys.exit(-1)
    
  
    job_run_env=sys.argv[1]
    print("creating spark session")
    
    spark= utils.get_spark_session(job_run_env)
    
    
    logger = Log4j(spark)
    
    logger.warn("CREATED SPARK SESSION")
    
    orders_df =DataReader.read_orders(spark,job_run_env)
    
    orders_filtered_df=DataManipulation.filter_closed_orders(orders_df)
    
    customers_df=DataReader.read_customers(spark, job_run_env)
    
    joined_df=DataManipulation.join_orders_customers(orders_filtered_df,customers_df)
    
    aggregated_results=DataManipulation.count_orders_state(joined_df)
    
    aggregated_results.show()
    
    logger.info("End of application")