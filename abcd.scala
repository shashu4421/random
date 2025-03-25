import org.apache.spark.sql.functions._

val joinCols = Seq("orderId") // Primary key column(s)

// Extract non-key columns dynamically
val dataCols = df1.columns.filterNot(joinCols.contains)

// Ensure both DataFrames have the same schema (cast if needed)
val df1Casted = df1.select(df1.columns.map(c => col(c).cast("string")): _*)
val df2Casted = df2.select(df2.columns.map(c => col(c).cast("string")): _*)

// Sort both DataFrames by `orderId`
val df1Sorted = df1Casted.orderBy("orderId")
val df2Sorted = df2Casted.orderBy("orderId")

// Perform an outer join on `orderId`
val dfJoined = df1Sorted.alias("df1")
  .join(df2Sorted.alias("df2"), joinCols, "outer")

// Generate a column that captures differences
val diffExprs = dataCols.map { colName =>
  when(trim(lower(col(s"df1.$colName"))) =!= trim(lower(col(s"df2.$colName"))) ||
       col(s"df1.$colName").isNull =!= col(s"df2.$colName").isNull,
       concat(lit(colName + ": "), col(s"df1.$colName"), lit(" → "), col(s"df2.$colName")))
    .otherwise(null)
}

val dfDiff = dfJoined
  .withColumn("Difference", when(
    trim(lower(col("df1.status"))) =!= trim(lower(col("df2.status"))) ||
    col("df1.status").isNull =!= col("df2.status").isNull,
    concat(lit("status: "), coalesce(col("df1.status"), lit("NULL")), lit(" → "), coalesce(col("df2.status"), lit("NULL")))
  ).otherwise(null))
  .filter(col("Difference").isNotNull)
  .select("orderId", "Difference")

// Concatenate differences into a single column
val dfDiff = dfJoined.withColumn("Differences", array_remove(array(diffExprs: _*), null))
  .filter(size(col("Differences")) > 0) // Keep only rows with differences
  .select(col("orderId"), col("Differences"))

// Show the differences
dfDiff.show(false)
