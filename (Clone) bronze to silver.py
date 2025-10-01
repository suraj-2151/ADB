# Databricks notebook source
# MAGIC %md
# MAGIC ## mount point for bronze

# COMMAND ----------

dbutils.fs.ls('mnt/bronze/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## mount point for silver

# COMMAND ----------

dbutils.fs.ls('mnt/silver/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading all files from bronze layer

# COMMAND ----------

customer_df=spark.read.csv(r'dbfs:/mnt/bronze/customer_table.csv',header=True,inferSchema=True)
order_df=spark.read.csv(r'dbfs:/mnt/bronze/order_table.csv',header=True,inferSchema=True)
product_df=spark.read.csv(r'dbfs:/mnt/bronze/product_table.csv',header=True,inferSchema=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Droping Duplicate rows from all tables

# COMMAND ----------

customer_df=customer_df.dropDuplicates(["CUSTOMERNAME",])
product_df=product_df.dropDuplicates(["PRODUCTCODE",])

# COMMAND ----------

# MAGIC %md
# MAGIC ##joining tables

# COMMAND ----------

join_1=order_df.join(customer_df,"CUSTOMERNAME")
join_2=join_1.join(product_df,"PRODUCTCODE")

# COMMAND ----------

# MAGIC %md
# MAGIC ##assigning name to final joined dataframe

# COMMAND ----------

df=join_2
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##dropping unwanted columns

# COMMAND ----------

df1=df.drop("PHONE","ADDRESSLINE1","ADDRESSLINE2","STATE","POSTALCODE","TERRITORY","ORDERLINENUMBER","MSRP")


# COMMAND ----------

# MAGIC %md
# MAGIC ##renaming columns with new names

# COMMAND ----------

df2=df1.withColumnRenamed("ORDERNUMBER","ORDER_NUMBER")\
       .withColumnRenamed("QUANTITYORDERED","QUANTITY_ORDERED")\
       .withColumnRenamed("PRICEEACH","PRICE_EACH")\
       .withColumnRenamed("PRODUCTLINE","VEHICLE_TOYS_NAME")\
       .withColumnRenamed("CUSTOMERNAME","ORGANISATION_NAME")\
       .withColumnRenamed("ORDERDATE","ORDER_DATE")\
       .withColumnRenamed("PRODUCTCODE","PRODUCT_CODE")\
       .withColumnRenamed("CONTACTLASTNAME","CONTACT_LAST_NAME")\
       .withColumnRenamed("CONTACTFIRSTNAME","CONTACT_FIRST_NAME")\
       .withColumnRenamed("DEALSIZE","DEAL_SIZE")
       

# COMMAND ----------

# MAGIC %md
# MAGIC ## assigning name to dataframe

# COMMAND ----------

sales_data=df2

# COMMAND ----------

# MAGIC %md
# MAGIC ##defining output path 

# COMMAND ----------

output_path= 'mnt/silver/sales_data/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## write opertaion to copy dataframe in to silver layer

# COMMAND ----------

sales_data.write.format('csv').option("header","true").mode("overwrite").save(output_path)


# COMMAND ----------

# silver_df=sales_data.toPandas()

# COMMAND ----------

# silver_df.to_csv(r'/dbfs/mnt/silver/silver_df',index=False)

