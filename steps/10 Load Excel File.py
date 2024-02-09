# import pandas lib as pd
import pandas as pd
from snowflake.snowpark.functions import *
from snowflake.snowpark.window import Window
from snowflake.snowpark import Session


from snowflake.snowpark.session import Session, FileOperation
import os
import json
def connect():
    with open('c:/test/creds.json') as f:
        connection_parameters = json.load(f)
        session = Session.builder.configs(connection_parameters).create()
        session.sql("USE DATABASE DEV_DB").collect()
        session.sql("USE SCHEMA DEV_SCHEMA").collect()
        return session

session = connect()

# read by default 1st sheet of an excel file
dataframe1 = pd.read_excel('@FROSTBYTE_RAW_STAGE/intro/order_detail.xlsx')

#print(dataframe1)
df =session.sql("select current_user()").collect()
