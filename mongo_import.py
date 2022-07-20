# coding: utf-8
"""
mongo_import.py
Mongo Import
Script that import Excel File content
in Mongo Database and manipulate the data
through Spark

kiye - 07/2022
"""
import config as CG
import tools as TL
import mylogger as ML
import data_operations as DO

def main():
    """
    Mongo Import
    """
    # conf file path
    conf_dir_path = '.\\conf'
    conf_file_name = 'app.json'
    # get config
    config = CG.config(conf_dir_path,conf_file_name)
    # store conf in variables
    app_conf = config.content.get('app')
    excel_conf = config.content.get('excel')
    mongo_conf = config.content.get('mongo')
    spk_conf = config.content.get('spark')
    output_conf = config.content.get('out')
    # variables
    excel_file_path = \
        f"{excel_conf.get('dir_name')}/{excel_conf.get('file_name')}"
    mongo_url = f"{mongo_conf.get('url')}:{mongo_conf.get('port')}"
    mongo_db_name = mongo_conf.get('database')
    mongo_coll_name = mongo_conf.get('collection')
    # set logger
    log = ML.MyLogger(app_conf.get('app_name'),app_conf.get('log_level'),\
        app_conf.get('formatter'))
    log.set_logs()
    try :        
        log.logger.info(f'Mongo Import.py : Start')    
        log.logger.info(f'Excel File Path : {excel_file_path}')
        log.logger.info(f'MongoDB URL : {mongo_url}')
        log.logger.info(f'Excel File content to MongoDB')
        # Excel to Mongo
        res_dict = TL.convert_excel_to_dict(log.logger,excel_file_path)
        log.logger.info(f'Nb of elements in dict : {len(res_dict)}')
        TL.import_dict_in_mongo(log.logger,mongo_url,mongo_db_name,\
            mongo_coll_name,res_dict)
        # spark operations
        log.logger.info(f'Spark Operations on MongoDB')
        mongo_in_uri = \
            f'mongodb://{mongo_url}/{mongo_db_name}.{mongo_coll_name}'
        mongo_out_uri = mongo_in_uri 
        spk_sess = TL.init_spark_mongo(log.logger,spk_conf.get('app_name'),\
            mongo_in_uri,mongo_out_uri)
        df_src = spk_sess.read.format(spk_conf.get('data_format'))\
            .option("database", mongo_db_name)\
                .option("collection", mongo_coll_name).load()
        DO.group_all_transactions_by_invoices(log.logger,df_src,\
            spk_conf.get('data_format'),mongo_db_name,\
            output_conf.get('coll1'),spk_conf.get('write_mode'))
        DO.which_product_sold_the_most(log.logger,df_src,\
            spk_conf.get('data_format'),mongo_db_name,\
            output_conf.get('coll2'),spk_conf.get('write_mode'))
        DO.which_customer_spent_the_most(log.logger,df_src,\
            spk_conf.get('data_format'),mongo_db_name,\
            output_conf.get('coll3'),spk_conf.get('write_mode'))
        DO.ratio_between_price_quantity_per_invoice(log.logger,df_src,\
            spk_conf.get('data_format'),mongo_db_name,\
            output_conf.get('coll4'),spk_conf.get('write_mode'))
        DO.distribution_product_countries(log.logger,df_src,\
            spk_conf.get('data_format'),mongo_db_name,\
            output_conf.get('coll5'),spk_conf.get('write_mode'))
        DO.distribution_prices(log.logger,df_src,\
            spk_conf.get('data_format'),mongo_db_name,\
            output_conf.get('coll6'),spk_conf.get('write_mode'))
        spk_sess.stop()
        log.logger.info(f'Mongo Import.py : End')
    except Exception as e :
        log.logger.error(e)

if __name__ == "__main__":
    main()