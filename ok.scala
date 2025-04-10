import org.apache.spark.sql.functions._
import spark.implicits._

// Step 1: Join on _c3 and eventName
val joinedDF = dfData.join(dfFields, dfData("_c3") === dfFields("eventName"))

// Step 2: Combine _c0 to _c2 into array of values
val withValuesArray = joinedDF.withColumn("values", array($"_c0", $"_c1", $"_c2"))

// Step 3: Zip fields with values => create a Map
val withZipped = withValuesArray.withColumn("zipped", map_from_arrays($"fields", $"values"))

// Optional: Explode map into columns
val finalDF = withZipped.select(
  $"_c3".alias("eventName"),
  $"zipped.*" // this will expand keys as column names
)

finalDF.show(false)
