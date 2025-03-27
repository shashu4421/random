import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("col1", StringType, true),
  StructField("col2", StringType, true),
  StructField("col3", StringType, true),
  StructField("col4", StringType, true),
  StructField("col5", StringType, true),
  StructField("col6", StringType, true) // Ensure all expected columns are defined
))

val df = spark.read
  .format("csv")
  .option("header", "true")
  .schema(schema)
  .load("abfss://path/to/your.csv")
