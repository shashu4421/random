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
