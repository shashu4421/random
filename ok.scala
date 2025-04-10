import org.apache.spark.sql.functions._

// Step 1: Create a list of _c* columns
val dataCols = joined.columns.filter(_.startsWith("_c")).sorted

// Step 2: Zip values and field names to a map
val zipColsUDF = udf((values: Seq[String], names: Seq[String]) => {
  names.zip(values).toMap
})

// Step 3: Create an array of values from the data columns
val dfWithArray = joined.withColumn("zippedMap", zipColsUDF(array(dataCols.map(col): _*), col("fields")))

// Step 4: Convert Map to Struct
val fieldsExample = joined.select("fields").as[Seq[String]].head
val structExpr = fieldsExample.map(f => s"zippedMap['$f'] as `$f`").mkString(",")

val renamedDF = dfWithArray.selectExpr(structExpr)

// Show renamed output
renamedDF.show(false)
