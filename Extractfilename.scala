import org.apache.spark.sql.functions._

val df = Seq(
  ("abfss://private@.../93005_SGAS_20250324_SGASOFIDESSA_OrderEvents_000001.csv.bz2?version=1743002524333?flength=492274")
).toDF("filepath")

// Extract filename using regex
val df_with_filename = df.withColumn("filename", regexp_extract(col("filepath"), ".*/(.+?)(\\?.*)?$", 1))

df_with_filename.show(truncate=false)
