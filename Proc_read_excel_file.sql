show stages in account;


list @hol_db.external.FROSTBYTE_RAW_STAGE/intro;


CREATE OR REPLACE PROCEDURE LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(file_path string,
worksheet_name string , target_table string) 
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION='3.10'
PACKAGES=('snowflake-snowpark-python','pandas','openpyxl')
HANDLER='main'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
from openpyxl import load_workbook
import pandas as pd
def main (session, file_path,worksheet_name, target_table):
    with SnowflakeFile.open(file_path,'rb') as f:
        workbook = load_workbook(f)
        sheet = workbook.get_sheet_by_name(worksheet_name)
        data =sheet.values
        
        # get the first line in file as header line 
        columns = next(data)[0:]
        # create a df based on the second and the subsequent lines of data
        df = pd.DataFrame(data, columns=columns)
        df2=session.create_dataframe(df)
        df2.write.mode("overwrite").save_as_table(target_table)
    return True
$$;

call LOAD_EXCEL_WORKSHEET_TO_TABLE_SP(build_scoped_file_url(@FROSTBYTE_RAW_STAGE,'intro/order_detail.xlsx') , 'order_detail', 'ORDER_DETAILS_TEST');

SELECT * FROM ORDER_DETAILS_TEST;

LIST @FROSTBYTE_RAW_STAGE/intro/order_detail.xlsx;