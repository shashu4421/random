import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("NormalizeTimestamps").getOrCreate()
import spark.implicits._

// Sample data with varying fractional lengths
val data = Seq(
  ("20250401T125904.184662"),   // 6 digits
  ("20250401T120800.656"),      // 3 digits
  ("20250401T120800.123456789"),// already 9 digits
  ("20250401T120800"),          // no fractional part
  ("20250401T120800.1")         // 1 digit
)

val df = data.toDF("eventTimestamp")

// Function to normalize to exactly 9 digits after dot
val normalizeTimestampUDF = udf((ts: String) => {
  val parts = ts.split("\\.")
  if (parts.length == 2) {
    val padded = parts(1).padTo(9, '0').take(9)
    s"${parts(0)}.$padded"
  } else {
    s"$ts.000000000"
  }
})

val dfNormalized = df.withColumn("eventTimestamp_nano", normalizeTimestampUDF($"eventTimestamp"))

dfNormalized.show(false)
