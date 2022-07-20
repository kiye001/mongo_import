# mongo_import

mongo_import is a simple Python script to import an Excel file in a Mongo Database and make some data processing via PySpark using this Mongo Database.

## Pre-requisite

* Mongo Database
* Python Environment 
* Spark Environment

*You have to make sure that your python environment is working and have all the libraries that are used in this script installed (pip install ...).*  
*Your Mongo Database should be accessible.*  
*According to Mongo Documentation, Mongo can work with Spark if you import JARs in your pyspark environment : mongo-java-driver and mongo-spark-connector (available under MAVEN).*   
*Put them under the directory "SPARK_HOME\jars" and it should work.*  
  
## Usage
You can put the Excel File that you want to process under the directory named "data". It can also be elsewhere on your environment...  
Go to *app.json* file under *conf* directory and report the directory name and the file name under "excel:dir_name" and "excel:file_name".   
To edit database parameters, report the database url, port number under the section "mongo:url" and "mongo:port".  
You can also put the name of database that you want to use/create and the name of the collection where the content of your Excel file will be stored under "mongo:database" and "mongo:collection".  
```json
"excel" : {
		"dir_name" : "data",
		"file_name" : "Online Retail.xlsx"
	},
"mongo":{
		"url":"TO_DEFINE",
		"port" : "TO_DEFINE",
		"database": "MY_DB",
		"collection" : "MY_COLL"
	}...	
```

Finally, you can run the script simply by calling it in your python environment.  


```shell
python ./mongo_import.py
```