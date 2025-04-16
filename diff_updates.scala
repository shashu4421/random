import uk.co.gresearch.spark.diff._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

def generateDiff(prodDF: DataFrame, devDF: DataFrame, eventType: String)(implicit spark: SparkSession): DataFrame = {
  import spark.implicits._

  val diff = {
    if (eventType == "MEOT" || eventType == "MEOF") {
      prodDF.diff(devDF,
        Seq("tradeID", "tradeKeyDate"),
        Seq("firmROEID", "CATReporterIMID", "eventTimestamp")
      )
    } else {
      prodDF.diff(devDF,
        Seq("orderID", "orderKeyDate"),
        Seq("firmROEID", "CATReporterIMID", "eventTimestamp")
      )
    }
  }.filter(col("diff") =!= lit("N"))

  // Step 1: Get base column names
  val allCols = prodDF.columns.diff(Seq("tradeID", "tradeKeyDate", "orderID", "orderKeyDate", "firmROEID", "CATReporterIMID", "eventTimestamp"))

  // Step 2: Identify changed columns
  val withChangedCols = diff.withColumn("changed_columns_raw", array(
    allCols.map { c =>
      when(col(s"${c}_left") =!= col(s"${c}_right"), lit(c))
    }: _*
  )).withColumn("changed_columns", expr("filter(changed_columns_raw, x -> x is not null)"))
    .drop("changed_columns_raw")

  // Step 3: Filter only changed rows
  val changedOnly = withChangedCols.filter($"diff" === "C")

  // Step 4: Create a new DataFrame with only changed columns per row
  val result = changedOnly.map { row =>
    val keyCols = Seq("diff") ++ row.schema.fieldNames.filterNot((name: String) => name.endsWith("_left") || name.endsWith("_right")).filter(_ != "changed_columns")
    val changedCols = row.getAs[Seq[String]]("changed_columns")

    val base = keyCols.map(k => k -> row.getAs[Any](k)).toMap
    val changed = changedCols.flatMap { c =>
      Seq(
        s"${c}_left" -> row.getAs[Any](s"${c}_left"),
        s"${c}_right" -> row.getAs[Any](s"${c}_right")
      )
    }.toMap

    val all = base ++ changed + ("changed_columns" -> changedCols)
    Row.fromSeq(all.values.toSeq)
  } (
    // Dynamic schema for the final output
    RowEncoder(StructType(
      (Seq("diff") ++
        prodDF.columns.filterNot(allCols.contains) ++
        allCols.flatMap(c => Seq(s"${c}_left", s"${c}_right")) ++
        Seq("changed_columns"))
        .distinct
        .map(name => StructField(name, StringType, true)) :+
      StructField("changed_columns", ArrayType(StringType, true), true)
    ))
  )

  result
}
