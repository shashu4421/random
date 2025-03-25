
// Generate a new column that captures differences
val diffExprs = dataCols.map { colName =>
  when(col(s"df1.$colName").isNull && col(s"df2.$colName").isNotNull, lit(s"$colName:NULL → " + col(s"df2.$colName")))
    .when(col(s"df1.$colName").isNotNull && col(s"df2.$colName").isNull, lit(s"$colName:" + col(s"df1.$colName") + " → NULL"))
    .when(col(s"df1.$colName") =!= col(s"df2.$colName"), lit(s"$colName:" + col(s"df1.$colName") + " → " + col(s"df2.$colName")))
    .otherwise(lit(null))
}

// Concatenate all differences into a single column
val dfDiff = dfJoined.withColumn("Differences", array_remove(array(diffExprs: _*), lit(null)))
  .filter(size(col("Differences")) > 0) // Remove rows with no differences
  .select(col("id"), col("Differences"))

// Show the diff report
dfDiff.show(false)
