import sys
from lib import DataManipulation,DataReader,Utils
from pyspark.sql.functions import *
from lib.logger import Log4j

#i am on featur1 branch
if __name__ == "__main__":
    if (len(sys.argv )< 2):
      print("Please specify the environment")
      sys.exit(-1)

    job_run_env = sys.argv[1]
    
    print("Creating spark session")
    spark = Utils.get_spark_session(job_run_env)  

    
    # Set log level to INFO
    spark.sparkContext.setLogLevel("WARN")
    print("Created SparkSession with INFO log level")

    #logger.info("Created spark session")
   
    orders_df = DataReader.read_orders(spark,job_run_env)
    print("Created orders df")
    orders_df.show()
   
    customers_df = DataReader.read_customers(spark,job_run_env)
    print("created customers df")

    filtered_df = DataManipulation.filter_closed_orders(orders_df)
    joined_df = DataManipulation.join_orders_customers(customers_df,filtered_df)
    grouped_df = DataManipulation.count_orders_state(joined_df)

    grouped_df.show()
    #logger.info("end of main")
