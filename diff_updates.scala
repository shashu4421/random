import uk.co.gresearch.spark.diff._

def generateDiff(prodDF: DataFrame, devDF: DataFrame, eventType: String): DataFrame = {
  var diff = spark.emptyDataFrame

  val diffOptions = DiffOptions.default
    .withChangedColumnsColumn("changedColumns") // <--- this adds the column with changed column names

  if (eventType == "MEOT" || eventType == "MEOF") {
    diff = prodDF.diff(
      devDF,
      Seq("tradeID", "tradeKeyDate"),
      Seq("firmROEID", "CATReporterIMID", "eventTimestamp"),
      diffOptions
    ).filter(col("diff") =!= lit("N"))
  } else {
    diff = prodDF.diff(
      devDF,
      Seq("orderID", "orderKeyDate"),
      Seq("firmROEID", "CATReporterIMID", "eventTimestamp"),
      diffOptions
    ).filter(col("diff") =!= lit("N"))
  }

  diff
}


import org.apache.spark.sql.functions._
import uk.co.gresearch.spark.diff._

def generateDiff(prodDF: DataFrame, devDF: DataFrame, eventType: String): DataFrame = {
  val keyCols = if (eventType == "MEOT" || eventType == "MEOF")
    Seq("tradeID", "tradeKeyDate")
  else
    Seq("orderID", "orderKeyDate")

  val compareCols = Seq("firmROEID", "CATReporterIMID", "eventTimestamp")

  val diff = prodDF.diff(devDF, keyCols, compareCols).filter(col("diff") =!= lit("N"))

  val changedColsExprs = compareCols.map { colName =>
    when(
      col(s"left_$colName").isNull && col(s"right_$colName").isNotNull ||
      col(s"left_$colName").isNotNull && col(s"right_$colName").isNull ||
      col(s"left_$colName") =!= col(s"right_$colName"),
      lit(colName)
    ).otherwise(null)
  }

  val withChangedCols = diff
  .withColumn("changedColumns_array", array_remove(array(changedColsExprs: _*), null))
  .withColumn("changedColumns", concat_ws(",", col("changedColumns_array")))
  .drop("changedColumns_array")


  withChangedCols
}


