import openpyxl
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, length, count_distinct, col, min, max, sum



spark = SparkSession.builder\
    .appName("Customer Segmentation")\
    .getOrCreate()

df = spark.read\
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("C:\\Users\\Owner\\Documents\\GitProjects\\spark\\data\\Online Retail 50000.csv")

df.show()



df_country_by_customerid = df.groupby('CustomerID')\
    .agg(count('Country'))\
    .alias('num_country')
df_country_by_customerid.show()


df_customerid_by_country = df.groupby('Country')\
    .agg(count('CustomerID'))\
    .alias('num_customerID')
df_customerid_by_country.show()
#print(df_customerid_by_country)



#print(df_country_by_customerid)
#print(df_country_by_customerid[df_country_by_customerid['counter'] > 1])

#valid_customers = df_country_by_customerid[df_country_by_customerid['counter'] == 1].CustomerID

#df = df[df.CustomerID.isin(valid_customers)]
##print(df.shape)
#print(f'# of unique customer ids, {len(df.CustomerID.unique())}')

df_country_by_customerid = df.groupby('CustomerID')\
                            .agg(count_distinct('Country').alias('num_distinct_country'))
#df_country_by_customerid.show()
df_country_by_customerid.filter(col('num_distinct_country') > 1 ).show()


#['InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate','UnitPrice', 'CustomerID', 'Country'],

#min_invoice_date = df.InvoiceDate.min();
#max_invoice_date = df.InvoiceDate.max();

df_date_range = df.agg(min("InvoiceDate").alias("min_invoice_date"), max("InvoiceDate").alias("max_invoice_date"))
df_date_range.show()

date_range = df_date_range.collect()[0]

window_a_start, window_a_stop = date_range['min_invoice_date'], date_range['min_invoice_date'] + pd.DateOffset(months=1)
window_b_start, window_b_stop = date_range['max_invoice_date'] - pd.DateOffset(months=1), date_range['max_invoice_date']
#window_a_start, window_a_stop = min_invoice_date, min_invoice_date + pd.DateOffset(months=1)
#window_b_start, window_b_stop = max_invoice_date - pd.DateOffset(months=1), max_invoice_date
print(window_a_start, window_a_stop)


#df['Monetary'] = df['Quantity'] *df['UnitPrice']
df =df.withColumn('Monetary', col('Quantity') * col('UnitPrice'))
#df_window_a =df[(df.InvoiceDate >= window_a_start) & (df.InvoiceDate <= window_a_stop)]
#df_window_b =df[(df.InvoiceDate >= window_b_start) & (df.InvoiceDate <= window_b_stop)]

df_window_a =df.filter( (col('InvoiceDate') >= window_a_start) & (col('InvoiceDate') <= window_a_stop))
df_window_b =df.filter( (col('InvoiceDate') >= window_b_start) & (col('InvoiceDate') <= window_b_stop))


monetary_a = df_window_a.groupby('CustomerID').agg(sum('Monetary').alias('Monetary_A'))
#monetary_b = df_window_b.groupby('CustomerID')['Monetary'].sum().reset_index()
monetary_b = df_window_b.groupby('CustomerID').agg(sum('Monetary').alias('Monetary_B'))


#df_monetary = pd.merge(monetary_a,monetary_b, on='CustomerID', how='left', suffixes=('_a', '_b'))

df_monetary = monetary_a.join(monetary_b, on='CustomerID', how='left')
#df_monetary.show()

df_monetary = df_monetary.filter((col('Monetary_B').isNotNull()) & (col('Monetary_A') > 0))
#df_monetary['Elasticity'] = df_monetary['Monetary_b'] / df_monetary['Monetary_a'] 
df_monetary = df_monetary.withColumn('Elasticity', col('Monetary_B')/ col('Monetary_A')) 
#print(df_monetary)
df_monetary.show()

df_monetary.write.mode("overwrite").option("header", "true").csv('data/Online_Retail')
df_monetary.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("data/Online_Retail_50000_ML")