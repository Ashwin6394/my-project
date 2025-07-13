from pyspark.sql.functions import col

df = df.withColumn("Revenue", 
    col("Fare_amount") + col("Extra") + col("MTA_tax") + 
    col("Improvement_surcharge") + col("Tip_amount") + 
    col("Tolls_amount") + col("Total_amount"))

df.groupBy("PULocationID").sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False)

df.groupBy("VendorID").avg("total_amount") \
  .withColumnRenamed("avg(total_amount)", "average_earning")

df.groupBy("payment_type").count() \
  .withColumnRenamed("count", "payment_count")
from pyspark.sql.functions import to_date

specific_date = "2020-01-15"
df_filtered = df.filter(to_date(df.tpep_pickup_datetime) == specific_date)

df_filtered.groupBy("VendorID").agg({
    "total_amount": "sum",
    "passenger_count": "sum",
    "trip_distance": "sum"
}).orderBy("sum(total_amount)", ascending=False).show(2)

df.groupBy("PULocationID", "DOLocationID").sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False).show(1)

from pyspark.sql.functions import unix_timestamp

current_time = df.select(unix_timestamp(col("tpep_pickup_datetime"))) \
                 .orderBy(unix_timestamp(col("tpep_pickup_datetime")), ascending=False) \
                 .first()[0]

df.withColumn("pickup_time", unix_timestamp(col("tpep_pickup_datetime"))) \
  .filter(col("pickup_time") > (current_time - 10)) \
  .groupBy("PULocationID").sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False)



