from lib import ConfigReader

#defining customers schema
def get_customers_schema():
    schema = "customer_id int,customer_fname string,customer_lname string,username string,password string,address string,city string,state string,pincode string"
    return schema

#defining orders schema
def get_orders_schmea():
    schema = "order_id int,order_date string,customer_id int,order_status string"
    return schema

#creating customers dataframe
def read_customers(spark,env):
    conf = ConfigReader.get_app_config(env)
    customers_filepath = conf["customers.file.path"]
    return spark.read \
    .format("csv") \
    .schema(get_customers_schema()) \
    .load(customers_filepath)

 #creating orders dataframe
def read_orders(spark,env):
    conf =ConfigReader.get_app_config(env)
    orders_filepath = conf["orders.file.path"]
    return spark.read \
    .format("csv") \
    .schema(get_orders_schmea()) \
    .load(orders_filepath)
