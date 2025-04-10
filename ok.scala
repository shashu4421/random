import org.apache.spark.sql.functions._

// Join both DataFrames where _c3 == eventName
val joinedDF = dfData.join(dfSchema, dfData("_c3") === dfSchema("eventName"))

// Extract _c columns (you can make this dynamic based on how many _c columns you expect)
val dataCols = dfData.columns.filter(_.startsWith("_c")).sorted  // ensure order like _c0, _c1,...

// UDF to zip column names with values and return as struct
val zipColsUDF = udf((values: Seq[String], names: Seq[String]) => {
  names.zip(values).toMap
})

// Convert row to array of values
val dfWithArray = joinedDF.withColumn("zipped", zipColsUDF(array(dataCols.map(col): _*), col("fields")))

// Explode the map back into columns
val renamedDF = dfWithArray.select(
  dfWithArray("zipped").as("z")
).selectExpr("z.*")
