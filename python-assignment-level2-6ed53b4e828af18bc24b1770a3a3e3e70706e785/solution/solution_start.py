import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False,
                        default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False,
                        default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False,
                        default="./input_data/starter/transactions/*/transactions.json")
    parser.add_argument('--output_location', required=False,
                        default="./output_data/outputs/")
    return vars(parser.parse_args())


def read_trans(spark, customers_location):
    df = spark.read.json(customers_location)
    df2 = df.select(df.customer_id, explode(df.basket).alias('nested'))
    trans = df2.withColumn('product_id', df2.nested.product_id).withColumn(
        'price', df2.nested.price).drop('nested')
    return trans


def main():
    params = get_params()
    spark = SparkSession.builder.master(
        "local[1]").appName('Spark Example').getOrCreate()
    trans = read_trans(spark, params['transactions_location'])
    cust = spark.read.option("header", True).csv(params['customers_location'])
    prod = spark.read.option("header", True).csv(params['products_location'])

    final = trans.join(cust, trans.customer_id == cust.customer_id, 'inner')\
        .join(prod, trans.product_id == prod.product_id, 'inner')\
        .select(trans.customer_id, cust.loyalty_score, trans.product_id, prod.product_category)\
        .withColumn('purchase_count', count(col('customer_id')).over(Window.partitionBy(col('customer_id'))))

    final.write.option("header", True) \
        .csv(params["output_location"])


if __name__ == "__main__":
    main()
