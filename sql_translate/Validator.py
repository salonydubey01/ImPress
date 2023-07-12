#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 22 18:20:58 2023

@author: salonydubey
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, to_date, when, monotonically_increasing_id, regexp_extract, explode, split, sum, instr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType,LongType, FloatType, DoubleType, DecimalType, DateType
from datetime import datetime
from presto import *
from pyhive import presto 
from impala.dbapi import connect as impala_connect
import logging
from datetime import datetime
import time 
import csv



# logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# # Define the schema for statistics DataFrame
statistics = {
    'executionid': '',
    'filename': '',
    'row_count_impala': '',
    'row_count_presto': '',
    'validate_rows': False,
    'execution_time_impala (sec)': '',
    'execution_time_presto (sec)': '',
    'variation_time (sec)': '',
    'schema_check': False,
    'numeric_columns_impala': [],
    'numeric_columns_presto': [],
    'sum_impala' :'',
    'sum_presto' :'',
    'min_impala' :'',
    'min_presto' :'',
    'max_impala' :'',
    'max_presto':'',
    'validate_datetime_format_impala':[],
    'validate_datetime_format_presto':[],
    'validate_datetime_format': '',
    'String_len_validation':'',
    'Validate_special_chars':'',
    'Null_count_impala':'',
    'Null_count_presto' :''
}

# Create an empty list to store statistics rows
statistics_rows = []





def validate_string_length_df1(df1, df2, column_list):
    len_stats = {}
    valid = True
    for column in column_list:
        lengths1 = df1.select(length(col(column)).alias(column)).groupBy().sum().collect()[0][0]
        lengths2 = df2.select(length(col(column)).alias(column)).groupBy().sum().collect()[0][0]
        if lengths1 == lengths2:
            len_stats[column] = True
        else:
            len_stats[column] = False
            valid = False
    return len_stats, valid

def validate_date_columns(df1, df2):
    date_format1 = df1.schema.fields[0].dataType
    date_format2 = df2.schema.fields[0].dataType
    if isinstance(date_format1, TimestampType) and isinstance(date_format2, TimestampType):
        return True
    else:
        return False
    
    
def get_data_from_impala(spark,impala_cursor, impalaquery):
    impala_start_date = time.time()
    impala_cursor.execute(impalaquery)
    impala_columns = impala_cursor.description
    cols=[]
    for index, column in enumerate(impala_columns): 
        cols.append(column[0])
    # result1 = [{columns[index][0]: column for index, column in enumerate(value)} for value in impala_cursor.fetchall()]
    impala_end_date = time.time()
    result1 = impala_cursor.fetchall()
    exec_time_impala = impala_end_date - impala_start_date
#    exec_time_impala = round(exec_time_impala,3)
    # statistics_schema['execution_time_impala (sec)'] = str(exec_time_impala)
    df1 = spark.createDataFrame(result1,cols)
    
    return df1, exec_time_impala


def get_data_from_presto(spark, presto_cursor, prestoquery):
    presto_start_date = time.time()
    presto_cursor.execute(prestoquery)
    presto_columns = presto_cursor.description
    # result2 = [{presto_columns[index][0]: column for index, column in enumerate(value)} for value in presto_cursor.fetchall()]
    cols=[]
    for index, column in enumerate(presto_columns): 
        cols.append(column[0])
    result2 = presto_cursor.fetchall()
    presto_end_date = time.time()
    exec_time_presto = presto_end_date - presto_start_date
    #exec_time_presto = round(exec_time_presto,3)
    # statistics_schema['execution_time_presto (sec)'] = str(exec_time_presto)
    df2 = spark.createDataFrame(result2,cols)
    
    return df2, exec_time_presto



def schema_check(df1, df2):
    if len(df1.columns) != len(df2.columns):
        log.info("Number of Columns are different")
        log.info("Impala Columns are {a}".format(a=len(df1.columns)) )
        log.info("Presto Columns are {a}".format(a=len(df2.columns)))
        # log.info("Impala Columns are {a}".format(a=df1.columns) )
        # log.info("Presto Columns are {a}".format(a=df2.columns))


        return False
    for col1, col2 in zip(df1.columns, df2.columns):
        column_imapla = next(dt for dt in df1.dtypes if dt[0] == col1)[1]
        column_presto = next(dt for dt in df2.dtypes if dt[0] == col2)[1]

        if column_imapla != column_presto:
            print(f"Data type mismatch for columns {col1} and {col2}.")
            log.info("Data types | Names of columns are different.")
            log.info("Impala Columns are {a}".format(a=df1.columns) )
            log.info("Presto Columns are {a}".format(a=df2.columns))
            return False

    return True

def validate_string_length_df1(df1,df2 , column_list1  , column_list12 ):
    print("entered in string validation")
    len_stats= {}
    valid = True
    lengths1=0
    lengths2=0
    for onecolumn in column_list1:
         
        lengths1 =lengths1+ df1.select(length(onecolumn)).groupBy().sum().collect()[0][0]
        
        print("lengths1 :",lengths1)
        
    for onecolumn2 in column_list12:
        lengths2 = lengths2+ df2.select(length(onecolumn2)).groupBy().sum().collect()[0][0]
        print(lengths2)
            
    if lengths1 == lengths2:
        valid = True 
    else:
        # len_stats[onecolumn] = False 
        valid = False

    return len_stats, valid  


def date_col_string(df):
    date_columns = {}
    columns=[]
    for col_name, data_type in df.dtypes:
        if data_type == 'date' or isinstance(data_type, DateType):
            if "datetype" not in date_columns.keys():
                date_columns["datetype"]=[]
            date_columns["datetype"].append(col_name)
            columns.append(col_name)
            continue
        else:
            df = df.withColumn("date check1", regexp_replace(col(col_name), r"[^0-9\/\-]", ""))
            df = df.withColumn("date check2", col("date check1").rlike(
                r"^\d{4}[/-]\d{1,2}[/-]\d{1,2}$|^\d{1,2}[/-]\d{1,2}[/-]\d{4}$|^\d{1,2}[/-]\d{4}[/-]\d{1,2}$"))
            if df.filter(col("date check2") == True).count() > 0:
                if "string" not in date_columns.keys():
                    date_columns["string"]=[]
                    
                date_columns["string"].append(col_name)
                columns.append(col_name)
                df = df.withColumn(col_name, col("date check1"))
    df = df.drop("date check1", "date check2")
    return df, columns

def save_data_in_csv(data):
    fieldnames = data[0].keys()
    output_path = "/Users/salonydubey/Desktop/validator_stats.csv"

    # Write the data to the CSV file
    with open(output_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
        # Write the header row
        writer.writeheader()
    
        # Write the data rows
        writer.writerows(data)



def extract_date_pattern(df, date_columns,prefix):
    for onecolumn in date_columns:
        print(onecolumn)
        df = df.withColumn(prefix+"_"+onecolumn+"_date_pattern",
                    when(col(onecolumn).rlike("^\\d{4}-\\d{2}-\\d{2}$"), "yyyy-MM-dd")
                    .when(col(onecolumn).rlike("^\\d{2}/\\d{2}/\\d{4}$"), "MM/dd/yyyy")
                    .when(col(onecolumn).rlike("^\\d{2}-\\d{2}-\\d{4}$"), "dd-MM-yyyy")
                    .otherwise("Unknown"))
    return df
        
def validate_date_pattern(df1, df2, columns_list1, columns_list2):
    df3 = df1.join(df2, 'ID')
    for onecol1, onecol2 in zip(columns_list1,columns_list2):
        colnd1 = "impala_"+onecol1+"_date_pattern"
        colnd2 = "presto_"+onecol2+"_date_pattern"

        df3 = df3.withColumn(onecol1+'_'+onecol2+'_DatePatternsMatch', col(colnd1) == col(colnd2))
        flags = df3.select(onecol1+'_'+onecol2+'_DatePatternsMatch').distinct()
        if flags.where(flags[onecol1 + "_" + onecol2 + "_DatePatternsMatch"] == False).count() > 0:
            print("The flags contain False")
            return False
        else:
            print("The flags do not contain False")
            continue
                
    return True


def check_null_values(df):
    """


    Parameters
    ----------
    df : TYPE
        DESCRIPTION.

    Returns
    -------
    l : TYPE
        DESCRIPTION.

    """

    # l = []
    total = 0
    for i in df.columns:
        if df.filter(col(i) == "").count() > 0:
            # l.append(i)
            total += df.filter(col(i) == "").count()

    return total


def check_special_char(checkdf):
    """


    Parameters
    ----------
    valid_across_allcol
    timestamp_columns
    df : TYPE
        DESCRIPTION.

    Returns
    -------
    l : TYPE
        DESCRIPTION.

    """
    
    special_char_pattern = r"[\w\s]"
    
    # Extract special characters from each column
    special_characters = []
    for column in checkdf.columns:
        # Apply the regular expression pattern and split the column by special characters
        special_chars = checkdf.select(explode(split(regexp_replace(col(column), special_char_pattern, ""), ""))).distinct().collect()
        special_characters.extend([row[0] for row in special_chars])


    # Print the unique special characters
    special_chars = (set(special_characters))
    print(special_chars)
    return special_chars
   


def validate_data_frames(df1, df2, executionId, filename, exec_time_impala, exec_time_presto):
    df1 = df1.withColumn('ID',monotonically_increasing_id())
    df2 = df2.withColumn('ID',monotonically_increasing_id())

    statistics['executionid'] = executionId
    statistics['filename'] = filename
    # print(len(df1))

    statistics['validate_rows'] = df1.count() == (df2.count())
    statistics['row_count_impala'] = df1.count()
    statistics['row_count_presto'] = df2.count()
    statistics['execution_time_impala (sec)'] = str(exec_time_impala)
    statistics['execution_time_presto (sec)'] = str(exec_time_presto)

    
################################ check schema ####################################
    schema_check_result = schema_check(df1, df2)    
    statistics['schema_check'] = schema_check_result
    print("schema_check_result : ", schema_check_result)
    if schema_check_result:
        string_columns_df1 = [field.name for field in df1.schema.fields if isinstance(field.dataType, StringType)]
        string_columns_df2 = [field.name for field in df2.schema.fields if isinstance(field.dataType, StringType)]

        print("string_columns_df : ",string_columns_df1)
        print("string_columns_df : ",string_columns_df2)

        string_Stats , valid =  validate_string_length_df1(df1,df2 ,string_columns_df1,string_columns_df2 )
        print("valid : ",valid)
        if valid :
            statistics['String_len_validation'] = True 
        else :
            statistics['String_len_validation'] = False 
            
############################## check numeric columns ###############################            
    numeric_columns_df1 = [field.name for field in df1.schema.fields
    if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType))]
    
    print(numeric_columns_df1)
    
    if numeric_columns_df1:
        statistics['numeric_columns_impala'] = ', '.join(numeric_columns_df1)
        # Calculate statistics for DataFrame 2

        # for column in numeric_columns_df1:
        #     min_value = df1.select(min(column)).first()[0]
        #     max_value = df1.select(max(column)).first()[0]
        #     mean_value = df1.select(mean(column)).first()[0]
        #     print(f"DataFrame 1 - Column: {column}\nMin Value: {min_value}\nMax Value: {max_value}\nMean Value: {mean_value}\n")

        # statistics['mean_impala'] = mean_value
        # # statistics['sum_impala'] = 
        # statistics['min_impala'] = min_value
        # statistics['max_impala'] = max_value
    else:
        statistics['numeric_columns_impala'] = 'N/A'

        
    numeric_columns_df2 =  [field.name for field in df2.schema.fields
    if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType))]
    
    print(numeric_columns_df2)
    
    if numeric_columns_df2:
        statistics['numeric_columns_presto'] = ', '.join(numeric_columns_df2)
        # Calculate statistics for DataFrame 2
        # for column in numeric_columns_df2:
        #     min_value = df2.select(min(column)).first()[0]
        #     max_value = df2.select(max(column)).first()[0]
        #     mean_value = df2.select(mean(column)).first()[0]
            
        # statistics['min_presto'] = min_value
        # statistics['max_presto'] = max_value
        # statistics['mean_presto'] = mean_value
    else:
        statistics['numeric_columns_presto'] = 'N/A'

############################## check date ##################################

    df1, impala_data_columns = date_col_string(df1)
    statistics['validate_datetime_format_impala']= ', '.join(impala_data_columns)
    if len(impala_data_columns)>= 1:
        impala_data_with_date_patterns = extract_date_pattern(df1, impala_data_columns, prefix ='impala')
        
        
    df2, presto_data_columns = date_col_string(df2)
    statistics['validate_datetime_format_presto']= ', '.join(presto_data_columns)
    if len(presto_data_columns)>= 1:
      presto_data_with_date_patterns= extract_date_pattern(df2, presto_data_columns,prefix = 'presto')
  
    if (len(impala_data_columns)>= 1) and (len(presto_data_columns)>= 1):
        datevaliated = validate_date_pattern(impala_data_with_date_patterns,presto_data_with_date_patterns,impala_data_columns,presto_data_columns)
    
        if not datevaliated:
            statistics['validate_datetime_format'] = False
        else:
            statistics['validate_datetime_format'] = True
    else:
            statistics['validate_datetime_format'] = ''

########################## check NULLS ###########################################

    impala_nulls = check_null_values(df1)
    if impala_nulls :
        statistics['Null_count_impala'] = impala_nulls    

    presto_nulls = check_null_values(df2)
    if presto_nulls:
        statistics['Null_count_presto'] = presto_nulls    


############################ Special Chars #######################################

    impala_spec =check_special_char(df1)
    presto_spec =check_special_char(df2)
    if impala_spec == presto_spec:
        statistics['Validate_special_chars'] = True
    else:
        statistics['Validate_special_chars'] = False

######################## Variation in time ######################################
      
    #statistics['variation_time (sec)'] = abs(exec_time_presto - exec_time_impala)

    statistics_rows.append(statistics.copy())
    return statistics_rows
    
    
def validate(df, spark , impala_cursor, presto_cursor):
    # log.info("validation started")


    df = spark.createDataFrame(df)

    

    df = df.collect()
    for onedf in df:
        # print(onedf)
        executionId = ''
        # onedf['executionId']
        print(executionId)
        filename=''
        # print(time.time())
        # log.info("Validation started for executionId ", str(executionId))
        log.info("Validation started for executionId {a} :".format(a=executionId))

        # filename = onedf['file']
        impalaquery = onedf['query']
        print(impalaquery)
        prestoquery = onedf['translated_query']
        
        
        try:
            sourcedata, exec_time_impala = get_data_from_impala(spark, impala_cursor, impalaquery)
            tragetdata , exec_time_presto= get_data_from_presto(spark, presto_cursor, prestoquery)
            
            final_df = validate_data_frames(sourcedata, tragetdata, executionId, filename,exec_time_impala,exec_time_presto)
        except Exception as e:
            log.error("Error occurred during validation: %s", str(e))
            continue

            # return
        
    print("File saving")
    try:
        save_data_in_csv(final_df)
    except Exception as e:
           log.error("Error occurred during saveing file: %s", str(e))
           return

