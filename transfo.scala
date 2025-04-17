import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

def normalizeTimestamp(col: Column): Column = {
  regexp_replace(
    regexp_replace(col, "(\\.0+)(@|$)", "$2"), // Remove .000... before @ or end
    "(\\.\\d*?)0+(@|$)", "$1$2"                // Remove trailing zeros after dot
  )
}

val dfWithNormalized = df.withColumn("col1_normalized", normalizeTimestamp(col("col1")))
                         .withColumn("col2_normalized", normalizeTimestamp(col("col2")))

// Now compare the normalized columns
val comparison = dfWithNormalized.withColumn("are_equal", 
  col("col1_normalized") === col("col2_normalized"))
