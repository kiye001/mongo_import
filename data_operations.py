# coding: utf-8
"""
data_operations.py
Data Operations Module 
Contains functions that compute data with Spark
kiye - 07/2022
"""
from numpy import outer
import pyspark.sql.functions as func
import logging as log
import tools as TL

def group_all_transactions_by_invoices(logger,df,out_format,\
    out_db_name,out_coll_name,write_mode):
    """
    Group All Transaction by invoices and store 
    result in out_db_name.out_coll_name
    Param : logger - logger
            df - dataframe
            out_format - output format - string 
            out_db_name - output database name - string
            out_coll_name - output collection name- string
            write_mode - output data write mode - string
    Return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try : 
        df = df.withColumn('subtotal',func.round(df['quantity']*df['unitprice'],3))
        # Group By Invoices 
        # Sum of all quantites in nb_elements 
        # customer_id_list to get all customer id in case of multiple customer per invoice
        # invoice_date_list to get all invoice dates id in case of multiple date per invoice
        # Total for the total per invoice
        df_invoices = df.groupBy(df['InvoiceNo'])\
            .agg(func.collect_set(df['customerid']).alias('customer_id_list'),\
                func.sum(df['Quantity']).alias('nb_elements'),\
                func.round(func.sum(df['subtotal']),3).alias('total'),\
                func.collect_set(df['InvoiceDate']).alias('invoice_date_list'))
        TL.save_df_on_mongo(logger,df_invoices,out_format,write_mode,\
            out_db_name,out_coll_name)
    except Exception as e:
        logger.error(e)

def which_product_sold_the_most(logger,df,out_format,\
    out_db_name,out_coll_name,write_mode):
    """
    Which product sold the most
    Param : logger - logger
            df - dataframe
            out_format - output format - string 
            out_db_name - output database name - string
            out_coll_name - output collection name- string
            write_mode - output data write mode - string
    Return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try : 
        # Group by Quantity and sum quantity to nb_products
        # and order By nb_products to get products that sold the most
        df_products = df.groupBy(df['stockcode'])\
            .agg(func.sum(df['Quantity']).alias('nb_products'))
        df_products = df_products.orderBy(df_products['nb_products'].desc())
        TL.save_df_on_mongo(logger,df_products,out_format,write_mode,\
            out_db_name,out_coll_name)
    except Exception as e:
        logger.error(e)

def which_customer_spent_the_most(logger,df,out_format,\
    out_db_name,out_coll_name,write_mode):
    """
    Which customer spend the most money? 
    Param : logger - logger
            df - dataframe
            out_format - output format - string 
            out_db_name - output database name - string
            out_coll_name - output collection name- string
            write_mode - output data write mode - string
    Return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try : 
        df = df.withColumn('subtotal',func.round(df['quantity']*df['unitprice'],3))
        # groupby customerids and sum order total
        df_cust_list = df.groupby('customerid')\
            .agg(func.round(func.sum(df['subtotal']),3)\
                .alias('total_spent_per_customer'))
        # order by descending total spent
        df_cust_list = df_cust_list\
            .orderBy(df_cust_list['total_spent_per_customer'].desc())
        TL.save_df_on_mongo(logger,df_cust_list,out_format,write_mode,\
            out_db_name,out_coll_name)
    except Exception as e:
        logger.error(e)

def ratio_between_price_quantity_per_invoice(logger,df,out_format,\
    out_db_name,out_coll_name,write_mode):
    """
    what's the ratio between price and quantity for each invoice?
    Param : logger - logger
            df - dataframe
            out_format - output format - string 
            out_db_name - output database name - string
            out_coll_name - output collection name- string
            write_mode - output data write mode - string
    Return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try : 
        # compute subtotal -> quantity * unit price
        df = df.withColumn('subtotal',\
            func.round(df['quantity']*df['unitprice'],3))
        # Group By invoice number sum total per invoice
        # and nb of elements per invoice
        df_res = df.groupby('InvoiceNo')\
            .agg(func.round(func.sum(df['subtotal']),3)\
                .alias('total_per_invoice'),\
               func.sum(df['Quantity']).alias('nb_elem_per_invoice'))
        # Compute price quantity ratio
        df_res = df_res.withColumn('price_quantity_ratio',\
            func.round(\
                df_res['total_per_invoice']/df_res['nb_elem_per_invoice'],3))
        TL.save_df_on_mongo(logger,df_res,out_format,write_mode,\
            out_db_name,out_coll_name)
    except Exception as e:
        logger.error(e)

def distribution_product_countries(logger,df,out_format,\
    out_db_name,out_coll_name,write_mode):
    """
    Distribution of each product 
    for each of the available countries
    Param : logger - logger
            df - dataframe
            out_format - output format - string 
            out_db_name - output database name - string
            out_coll_name - output collection name- string
            write_mode - output data write mode - string
    Return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try : 
        # total of product per country
        df_dist_prd_ctry = df.groupby(['StockCode','country'])\
            .agg(func.sum(df['quantity']).alias('total_per_country'))
        # total of products
        df_dist_prd = df.groupby(['StockCode'])\
            .agg(func.sum(df['quantity']).alias('total_per_products'))
        # join dataframe to have all the information in the same df
        df_dist_prd_ctry = df_dist_prd_ctry.join(df_dist_prd,\
            ['StockCode'],'left')
        # ratio quantity product in country / total quantity of products
        df_dist_prd_ctry = df_dist_prd_ctry.withColumn('country_part',\
            func.round(df_dist_prd_ctry['total_per_country']\
                /df_dist_prd_ctry['total_per_products'],3))
        df_dist_prd_ctry = df_dist_prd_ctry\
            .orderBy(df_dist_prd_ctry['country_part'].asc())
        df_dist_prd_ctry = df_dist_prd_ctry\
            .orderBy(df_dist_prd_ctry['country'])
        TL.save_df_on_mongo(logger,df_dist_prd_ctry,out_format,write_mode,\
            out_db_name,out_coll_name)
    except Exception as e:
        logger.error(e)

def distribution_prices(logger,df,out_format,\
    out_db_name,out_coll_name,write_mode):
    """
    distribution of prices
    Param : logger - logger
            df - dataframe
            out_format - output format - string 
            out_db_name - output database name - string
            out_coll_name - output collection name- string
            write_mode - output data write mode - string
    Return None
    """
    if type(logger) not in [log.Logger, log.RootLogger]:
        print('Logger error...')
        return None
    try : 
        # select stock Code and Unit Price
        df_price_dist = df.select(['StockCode','UnitPrice']).distinct()
        # order By descending Unit Price
        df_price_dist = df_price_dist\
            .orderBy(df_price_dist['UnitPrice'].desc())
        TL.save_df_on_mongo(logger,df_price_dist,out_format,write_mode,\
            out_db_name,out_coll_name)
    except Exception as e:
        logger.error(e)