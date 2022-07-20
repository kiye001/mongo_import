# coding: utf-8
"""
tools.py
Tools module
Contains various functions that will be used in the script

kiye - 07/2022
"""
import pymongo
import pandas as pd
import json
from pyspark.sql import SparkSession
import logging as log

def convert_excel_to_dict(logger,excel_file_path):
    """
    Convert excel file to dict
    param : logger - logger
            excel_file_path - String
    return dict
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    if excel_file_path is not None:
        try :
            df_xl = pd.read_excel(excel_file_path)
            tmp = df_xl.T.to_json(date_format = 'iso')
            return json.loads(tmp)
        except FileNotFoundError : 
            logger.error(f'{excel_file_path} not found')
    else:
        logger.error(f'Excel file path is not defined')

def import_dict_in_mongo(logger, mongo_url,mongo_db_name,\
    mongo_collection_name,dict_to_add):
    """
    Import dictionary content in mongo
    Param : logger - logger
            mongo_url - Mongo Database url-string
            mongo_db_name - Mongo Database name - string
            mongo_collection_name - Mongo Collection Name - string
            dict_to_add - dict
    return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try :
        client = pymongo.MongoClient(f'mongodb://{mongo_url}/')
        if mongo_db_name in client.list_database_names():
            logger.info(f'Data base {mongo_db_name} exists...')
        db = client.get_database(mongo_db_name)
        if mongo_collection_name in db.list_collection_names():
            logger.info(f"Collection {mongo_collection_name} "
            f"in data base {mongo_db_name} exists...")
            logger.info(f'It will be dropped before adding data')
            db.drop_collection(mongo_collection_name)
        collection = db.get_collection(mongo_collection_name)
        collection.insert_many(dict_to_add.values())
        logger.info(f'Elements in dict : {len(dict_to_add)}')
        logger.info(f"Document inserted in collection "
        f"{mongo_collection_name} : {collection.count_documents({})}""")
    except Exception as e:
        logger.error(f'Error : {e}')
    
def init_spark_mongo(logger,app_name, mongo_input_uri,mongo_output_uri):
    """
    Init Spark Session with MongoDB specific parameters
    Param : logger - logger
            app_name - Spark App Name - string 
            mongo_input_uri - Mongo input uri - string 
            mongo_output_uri - Mongo output uri - string 
    Return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try :
        my_spark_sess = SparkSession.builder.appName(app_name)\
            .config("spark.mongodb.input.uri", mongo_input_uri)\
            .config("spark.mongodb.output.uri", mongo_output_uri)\
            .getOrCreate()
        return my_spark_sess
    except Exception as e:
        logger.error(e)

def save_df_on_mongo(logger,df,out_format,write_mode,out_db_name,\
    out_coll_name):
    """
    Save dataframe content on MongoDB 
    Param : logger - logger
            df - Spark Dataframe  
            out_format - data output format - string
            write_mode - output data write mode - string
            out_db_name : output database name - string
            out_coll_name : output collection name - string
    Return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try :
        df.write.format(out_format).mode(write_mode)\
            .option("database",out_db_name)\
            .option("collection",out_coll_name).save()
    except Exception as e:
        logger.error(e)