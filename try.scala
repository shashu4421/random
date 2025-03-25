import org.apache.spark.sql.functions._

val joinCols = Seq("orderId") // Primary key column(s)

// Extract non-key columns dynamically
val dataCols = df1.columns.filterNot(joinCols.contains)

// Ensure both DataFrames have the same schema (cast everything to string)
val df1Casted = df1.select(df1.columns.map(c => col(c).cast("string").alias(c)): _*)
val df2Casted = df2.select(df2.columns.map(c => col(c).cast("string").alias(c)): _*)

// Sort both DataFrames by `orderId`
val df1Sorted = df1Casted.orderBy("orderId")
val df2Sorted = df2Casted.orderBy("orderId")

// Perform an outer join on `orderId`
val dfJoined = df1Sorted.alias("df1")
  .join(df2Sorted.alias("df2"), joinCols, "outer")

// Generate a struct column that contains column differences
val diffStructs = dataCols.map { colName =>
  struct(lit(colName).alias("Column"),
         col(s"df1.$colName").alias("Old_Value"),
         col(s"df2.$colName").alias("New_Value"))
    .alias(colName)
}

// Add all differences as an array of structs
val dfWithDiffs = dfJoined.withColumn("Diffs", array(diffStructs: _*))
  .withColumn("Diffs", expr("filter(Diffs, d -> d.Old_Value != d.New_Value OR d.Old_Value IS NULL OR d.New_Value IS NULL)"))

// Explode differences into multiple rows
val dfExploded = dfWithDiffs
  .withColumn("Difference", explode(col("Diffs")))
  .select(
    col("orderId"),
    col("Difference.Column").alias("Column"),
    col("Difference.Old_Value").alias("Old_Value"),
    col("Difference.New_Value").alias("New_Value")
  )

// Show the results
dfExploded.show(false)
