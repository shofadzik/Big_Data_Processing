from flask import Flask
from subprocess import check_output
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
import os
from markupsafe import escape

app = Flask(__name__)

@app.route("/")
def hello_world():
    spark = SparkSession.builder.appName('GCSFilesRead').config("spark.jars", os.getcwd()+"/scripts/jars/gcs-connector-hadoop2-latest.jar").getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', os.getcwd()+"/scripts/cred/data-engineering-service-account.json")

    df = spark.read.option("header",True).csv(os.getcwd()+"/scripts/covid.csv")
    df.write.json(os.getcwd()+"/scripts/result")
    return("done")

@app.route('/orders/<username>')
def orders(username):
    spark = SparkSession.builder.appName('GCSFilesRead').config("spark.jars", os.getcwd()+"/scripts/jars/gcs-connector-hadoop2-latest.jar").getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', os.getcwd()+"/scripts/cred/data-engineering-service-account.json")

    df = spark.read.option("header",True).csv(os.getcwd()+"/scripts/source/orders.csv")
    df.write.json(os.getcwd()+f"/scripts/result/orders/{escape(username)}")
    return f'User {escape(username)}'

@app.route('/order_details/<username>')
def order_details(username):
    spark = SparkSession.builder.appName('GCSFilesRead').config("spark.jars", os.getcwd()+"/scripts/jars/gcs-connector-hadoop2-latest.jar").getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', os.getcwd()+"/scripts/cred/data-engineering-service-account.json")

    df = spark.read.option("header",True).csv(os.getcwd()+"/scripts/source/order_details.csv")
    df.write.json(os.getcwd()+f"/scripts/result/order_details/{escape(username)}")
    return f'User {escape(username)}'

@app.route('/categories/<username>')
def categories(username):
    spark = SparkSession.builder.appName('GCSFilesRead').config("spark.jars", os.getcwd()+"/scripts/jars/gcs-connector-hadoop2-latest.jar").getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', os.getcwd()+"/scripts/cred/data-engineering-service-account.json")

    df = spark.read.option("header",True).csv(os.getcwd()+"/scripts/source/categories.csv")
    df.write.json(os.getcwd()+f"/scripts/result/categories/{escape(username)}")
    return f'User {escape(username)}'

@app.route('/orders_joined/<username>')
def orders_joined(username):
    spark = SparkSession.builder.appName('GCSFilesRead').config("spark.jars", os.getcwd()+"/scripts/jars/gcs-connector-hadoop2-latest.jar").getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', os.getcwd()+"/scripts/cred/data-engineering-service-account.json")

    df_orders = spark.read.option("header",True).csv(os.getcwd()+"/scripts/source/orders.csv")
    df_order_details = spark.read.option("header",True).csv(os.getcwd()+"/scripts/source/order_details.csv")

    df_join = df_orders.join(df_order_details, "order_id", "left_outer")
    df_join.write.json(os.getcwd()+f"/scripts/result/orders_joined/{escape(username)}")
    return f'User {escape(username)}'


if __name__ == "__main__":
  app.run()


"""
karena tidak pake gcs berikut yang bisa di delete :

from google.cloud import storage

.config("spark.jars", os.getcwd()+"/scripts/jars/gcs-connector-hadoop2-latest.jar")

spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
spark._jsc.hadoopConfiguration().set('google.cloud.auth.service.account.json.keyfile', os.getcwd()+"/scripts/cred/data-engineering-service-account.json")

"""