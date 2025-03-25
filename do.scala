import org.apache.spark.sql.functions._

val joinCols = Seq("orderId") // Primary key column(s)

// Extract non-key columns dynamically
val dataCols = df1.columns.filterNot(joinCols.contains)

// Ensure both DataFrames have the same schema
val df1Casted = df1.select(df1.columns.map(c => col(c).cast("string")): _*)
val df2Casted = df2.select(df2.columns.map(c => col(c).cast("string")): _*)

// Sort both DataFrames by `orderId`
val df1Sorted = df1Casted.orderBy("orderId")
val df2Sorted = df2Casted.orderBy("orderId")

// Perform an outer join on `orderId`
val dfJoined = df1Sorted.alias("df1")
  .join(df2Sorted.alias("df2"), joinCols, "outer")

// Generate a column that captures differences dynamically for all columns
val diffExprs = dataCols.map { colName =>
  when(trim(lower(col(s"df1.$colName"))) =!= trim(lower(col(s"df2.$colName"))) ||
       col(s"df1.$colName").isNull =!= col(s"df2.$colName").isNull,
       concat(lit(colName + ": "), coalesce(col(s"df1.$colName"), lit("NULL")), lit(" → "), coalesce(col(s"df2.$colName"), lit("NULL"))))
    .otherwise(null)
}

// Concatenate differences into a single column
val dfDiff = dfJoined.withColumn("Differences", array_remove(array(diffExprs: _*), null))
  .filter(size(col("Differences")) > 0) // Keep only rows with differences
  .select(col("orderId"), col("Differences"))

// Show the differences
dfDiff.show(false)

// Log the differences to Azure Storage
dfDiff.write
  .mode("overwrite")
  .option("header", "true")
  .csv("abfss://<container>@<storage-account>.dfs.core.windows.net/diff_report")

println("Comparison completed. Differences saved to Azure Storage.")

