val dataCols = joined.columns.filter(_.startsWith("_c")).sorted

val zipColsUDF = udf((values: Seq[String], names: Seq[String]) => {
  names.zip(values).toMap
})

val dfWithArray = joined.withColumn("zippedMap", zipColsUDF(array(dataCols.map(col): _*), col("fields")))

val fieldsExample = joined.select("fields").as[Seq[String]].head
val structExpr = fieldsExample.map(f => s"zippedMap['$f'] as `$f`")

val renamedDF = dfWithArray.selectExpr(structExpr: _*)

renamedDF.show(false)
