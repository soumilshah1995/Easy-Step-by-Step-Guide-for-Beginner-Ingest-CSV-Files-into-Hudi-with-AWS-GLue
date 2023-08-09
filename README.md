
Easy Step by Step Guide for Beginner Ingest CSV Files into Hudi with AWS GLue

![Capture](https://github.com/soumilshah1995/Easy-Step-by-Step-Guide-for-Beginner-Ingest-CSV-Files-into-Hudi-with-AWS-GLue/assets/39345855/c2df22e6-0be7-43c6-9927-2ef88b3390a1)


# Data Generator Code 
```
import datetime
import json
import random
import boto3
import os
import io
from io import BytesIO, StringIO
import uuid
import time
import csv
from datetime import datetime
from faker import Faker
from dotenv import load_dotenv

load_dotenv("../dev.env")


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket, aws_access_key_id, aws_secret_access_key, region_name):

        self.BucketName = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:

            response = self.client.put_object(
                ACL="private", Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            print("Error : {} ".format(e))
            return "error"

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def __repr__(self):
        return "AWS S3 Helper class "


global faker
global helper

faker = Faker()
helper = AWSS3(
    aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
    region_name=os.getenv("DEV_REGION"),
    bucket=os.getenv("BUCKET")
)


def generate_fake_data():
    fake = Faker()

    # Generate fake orders and customers data
    orders = []
    customers = []

    for i in range(1, 20):
        order_id = uuid.uuid4().__str__()
        customer_id = uuid.uuid4().__str__()

        order = {
            "orderid": order_id,
            "customer_id": customer_id,
            "ts": datetime.now().isoformat().__str__(),
            "order_value": random.randint(10, 1000).__str__(),
            "priority": random.choice(["LOW", "MEDIUM", "URGENT"])
        }
        orders.append(order)

        customer = {
            "customer_id": customer_id,
            "name": fake.name(),
            "state": fake.state(),
            "city": fake.city(),
            "email": fake.email(),
            "ts": datetime.now().isoformat().__str__()
        }
        customers.append(customer)

    return orders, customers


def dump_csv_to_s3(data, key_prefix):
    csv_data = [list(data[0].keys())] + [list(item.values()) for item in data]

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(csv_data)

    # Convert the CSV data to bytes
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # Upload the CSV file to S3
    file_key = f'{key_prefix}/{uuid.uuid4().__str__()}.csv'
    helper.put_files(Response=csv_bytes, Key=file_key)


if __name__ == "__main__":
    orders, customers = generate_fake_data()

    # Dump orders and customers data to CSV files and upload to S3
    dump_csv_to_s3(orders, "raw/orders")
    dump_csv_to_s3(customers, "raw/customers")

```

# Glue job 
 Add these Paramerts  
 
 ![image](https://github.com/soumilshah1995/Easy-Step-by-Step-Guide-for-Beginner-Ingest-CSV-Files-into-Hudi-with-AWS-GLue/assets/39345855/1062b30c-861c-4f3d-8b5a-91d44abd117e)

```
"""
Team Leda: Soumil Nitin Shah
Developer : Divyansh Patel

# Configurations
key                           value
--additional-python-modules  faker==11.3.0
--conf                        spark.serializer=org.apache.spark.serializer.KryoSerializer
                             --conf spark.sql.hive.convertMetastoreParquet=false
                             --conf spark.sql.hive.convertMetastoreParquet=false
                             --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog
                             --conf spark.sql.legacy.pathOptionBehavior.enabled=true
                             --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
--datalake-formats           hudi
"""

# Import necessary modules
try:
    import os, uuid, sys, boto3, time, sys
    from pyspark.sql.functions import lit, udf
    import sys
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
except Exception as e:
    print("Modules are missing : {} ".format(e))

# Get command-line arguments
args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',


    ],
)
# Create a Spark session
spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

# Create a Spark context and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)


def upsert_hudi_table(glue_database, table_name, record_id, precomb_key, table_type, spark_df, partition_fields,
                      enable_partition, enable_cleaner, enable_hive_sync, enable_clustering,
                      enable_meta_data_indexing,
                      use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert', clustering_column='default'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        partition_fields this is used to parrtition data
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether or not to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": partition_fields,
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    hudi_clustering = {
        "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
        "hoodie.clustering.inline": "true",
        "hoodie.clustering.plan.strategy.sort.columns": clustering_column,
        "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",
        "hoodie.clustering.plan.strategy.small.file.limit": "629145600"
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    # Define a dictionary with the Fiel Size
    hudi_file_size = {
        "hoodie.parquet.max.file.size": 512 * 1024 * 1024,  # 512MB
        "hoodie.parquet.small.file.limit": 104857600,  # 100MB
    }

    hudi_meta_data_indexing = {
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.index.async": "true",
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.metadata.index.check.timeout.seconds": "60",
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider"
    }

    if enable_meta_data_indexing == True or enable_meta_data_indexing == "True" or enable_meta_data_indexing == "true":
        for key, value in hudi_meta_data_indexing.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    if enable_clustering == True or enable_clustering == "True" or enable_clustering == "true":
        for key, value in hudi_clustering.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    for key, value in hudi_file_size.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:
        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)


def read_data_s3(path, format, table_name):
    """
    Reads data from an S3 bucket using AWS Glue and returns a Spark DataFrame.

    Args:
        path (str): S3 bucket path where data is stored
        format (str): file format of the data (e.g., parquet)
        table_name (str): name of glue table
    Returns:
        spark_df (pyspark.sql.DataFrame): Spark DataFrame containing the data
    """

    transformation_ctx = f"hudi_job_{table_name}"

    if format == "parquet" or format == "json":

        job = args["JOB_NAME"]
        print(f'Job Name : {job}')

        # create a dynamic frame from the S3 bucket using AWS Glue
        glue_df = glueContext.create_dynamic_frame.from_options(
            format_options={},
            connection_type="s3",
            format=format,
            connection_options={
                "paths": [path],
                "recurse": True,
            },
            transformation_ctx=transformation_ctx,
        )

        # convert dynamic frame to Spark DataFrame
        spark_df = glue_df.toDF()

        # print the first few rows of the DataFrame
        print(spark_df.show())

        return spark_df

    elif format == "csv":
        glue_df = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [path],
                "recurse": True,
            },
            transformation_ctx=transformation_ctx,
        )
        spark_df = glue_df.toDF()
        print(spark_df.show())
        return spark_df


def run():
    # Read data from S3
    spark_df = read_data_s3(
        path="s3://hudi-learn-demo-1995/raw/customers/",
        format="csv",
        table_name="customers"
    )

    # Upsert data to Hudi table
    upsert_hudi_table(
        glue_database="hudidb",
        table_name="customers",
        record_id="customer_id",
        precomb_key="ts",
        table_type='COPY_ON_WRITE',
        partition_fields="state",
        method='bulk_insert',
        index_type='BLOOM',
        enable_partition=True,
        enable_cleaner=True,
        enable_hive_sync=True,
        enable_clustering='False',
        clustering_column='default',
        enable_meta_data_indexing='true',
        use_sql_transformer=True,
        sql_transformer_query='SELECT * FROM temp',
        target_path="s3://hudi-learn-demo-1995/silver/table_name=customers/",
        spark_df=spark_df,
    )

    # Commit the job
    job.commit()


run()

```


