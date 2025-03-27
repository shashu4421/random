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

import org.apache.spark.sql.types._

val maxCols = 10 // Set this to the highest number of columns you expect

val schema = StructType((1 to maxCols).map(i => StructField(s"col$i", StringType, true)))

val df = spark.read
  .format("csv")
  .option("header", "false") // No headers
  .option("delimiter", ",") // Explicit delimiter
  .schema(schema) // Use maximum possible columns
  .load("abfss://path/to/your.csv")

df.show(false) // Print full rows



import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.util.Try

val spark = SparkSession.builder().appName("LoadIfExists").getOrCreate()

def loadIfExists(path: String): DataFrame = {
  Try(spark.read.option("header", "true").csv(path)).getOrElse {
    println(s"Path does not exist: $path, returning empty DataFrame")
    spark.emptyDataFrame
  }
}

// Example Usage
val df = loadIfExists("path/to/file.csv")
df.show()


