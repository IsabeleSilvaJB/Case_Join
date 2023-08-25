# Databricks notebook source
import psycopg2

def conecta_db():
  con = psycopg2.connect(host='psql-mock-database-cloud.postgres.database.azure.com', 
                         database='ecom1692840458298ymitjshyedrfjlvq',
                         user='afesdunqrmfllihdbzrnacwp@psql-mock-database-cloud', 
                         password='rekuivjzextaqdbnlabkitie')
  return con

# COMMAND ----------

def consultar_db(sql):
  con = conecta_db()
  cur = con.cursor()
  cur.execute(sql)
  recset = cur.fetchall()
  tables=[]
  for rec in recset:
      for table_name in rec:
        tables.append(table_name)
  
  return tables

tables = consultar_db("select table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables= tables[0:8]
tables

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession


database_host = "psql-mock-database-cloud.postgres.database.azure.com"
database_port = "5432" # update if you use a non-default port
database_name = "ecom1692817202223mzgwryukwxpjowao"
#table = "customers"
user = "fbdcebxotbvbdjdkbgnpbwyr@psql-mock-database-cloud"
password = "epznsbacfhkvgklpcclrkegw"
url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"
driver = "org.postgresql.Driver"

spark=SparkSession.builder.master('local[*]').appName("teste").getOrCreate()

for table in tables:
# COMMAND ----------
  globals()['df_%s' % table] = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .load()
  )
  globals()['df_%s' % table].write.parquet(("/Join_case/{}.parquet").format(table))

# COMMAND ----------

for table in tables:
# COMMAND ----------
  globals()['df_%s' % table] = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .load()
  )
  globals()['df_%s' % table].write.format('parquet').save(("/Join_case/{}.parquet").format(table))
 

# COMMAND ----------

df_customers.write.saveAsTable("customers")

# COMMAND ----------

df_customers.write.format("delta").mode("overwrite").save(f"dbfs:/Join_case/customers")

# COMMAND ----------

from delta.tables import *

deltaTableCustomers = DeltaTable.forPath(spark, 'dbfs:/Join_case/customers')
dfUpdates = spark.read.format("parquet").load(f"dbfs:/Join_case/customers.parquet")

#dfUpdates = deltaTableCustomersUpdates.toDF()

deltaTableCustomers.alias('customers') \
  .merge(
    dfUpdates.alias('updates'),
    'customers.customer_number = updates.customer_number'
  ) \
  .whenMatchedUpdate(set =
    {
      "customer_name": "updates.customer_name",
      "contact_last_name": "updates.contact_last_name",
      "contact_first_name": "updates.contact_first_name"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "customer_number": "updates.customer_number",
      "contact_last_name": "updates.contact_last_name"
      
    }
  ).whenNotMatchedBySourceDelete() \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC No notebook crie código Spark que respondam as seguintes perguntas:
# MAGIC - Qual país possui a maior quantidade de itens cancelados?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC country,
# MAGIC COUNT(o.status)
# MAGIC  FROM parquet.`/Join_case/customers.parquet` as c
# MAGIC  INNER JOIN parquet.`/Join_case/orders.parquet` as o
# MAGIC  ON c.customer_number = o.customer_number
# MAGIC WHERE lower(o.status) = "cancelled"
# MAGIC GROUP BY 1
# MAGIC ORDER BY 2 DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC status,
# MAGIC year(order_date),
# MAGIC COUNT(o.status)
# MAGIC  FROM parquet.`/Join_case/orders.parquet` as o
# MAGIC GROUP BY 1,2
# MAGIC ORDER BY 2 desc 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Qual o faturamento da linha de produto mais vendido, considere como os itens
# MAGIC Shipped, cujo o pedido foi realizado no ano de 2005?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC SUM(pa.amount), 
# MAGIC pl.product_line,
# MAGIC COUNT(distinct(o.order_number)),
# MAGIC year(o.order_date),
# MAGIC lower(o.status)
# MAGIC  FROM parquet.`/Join_case/product_lines.parquet` as pl
# MAGIC  INNER JOIN parquet.`/Join_case/products.parquet` as p
# MAGIC  ON pl.product_line = p.product_line
# MAGIC  INNER JOIN parquet.`/Join_case/orderdetails.parquet` as od
# MAGIC  ON od.product_code = p.product_code
# MAGIC  INNER JOIN parquet.`/Join_case/orders.parquet` as o
# MAGIC  ON od.order_number = o.order_number
# MAGIC INNER JOIN parquet.`/Join_case/payments.parquet` as pa
# MAGIC  ON pa.customer_number = o.customer_number
# MAGIC
# MAGIC WHERE lower(o.status) = "shipped"
# MAGIC AND year(o.order_date)="2005"
# MAGIC GROUP BY 2,4,5
# MAGIC ORDER BY 1 DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Nome, sobrenome e e-mail dos vendedores do Japão, o local-part do e-mail
# MAGIC deve estar mascarado.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC first_name,
# MAGIC last_name,
# MAGIC concat(sha1(replace(email, '@classicmodelcars.com', '')),'@classicmodelcars.com') as email
# MAGIC FROM parquet.`/Join_case/employees.parquet` as em
# MAGIC INNER JOIN parquet.`/Join_case/offices.parquet` as o
# MAGIC ON o.office_code = em.office_code 
# MAGIC WHERE lower(o.country) = "japan"
# MAGIC
# MAGIC ORDER BY 2 desc 
# MAGIC

# COMMAND ----------





# COMMAND ----------


