import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

def generateDiffReport(df1: DataFrame, df2: DataFrame, joinCols: Seq[String]): DataFrame = {
  
  // Extract non-key columns dynamically
  val dataCols = df1.columns.filterNot(joinCols.contains)

  // Ensure both DataFrames have the same schema (cast everything to string)
  val df1Casted = df1.select(df1.columns.map(c => col(c).cast("string").alias(c)): _*)
  val df2Casted = df2.select(df2.columns.map(c => col(c).cast("string").alias(c)): _*)

  // Sort both DataFrames by join columns
  val df1Sorted = df1Casted.orderBy(joinCols.map(col): _*)
  val df2Sorted = df2Casted.orderBy(joinCols.map(col): _*)

  // Perform an outer join on the join columns
  val dfJoined = df1Sorted.alias("df1")
    .join(df2Sorted.alias("df2"), joinCols, "outer")

  // Generate a struct column that contains column differences
  val diffStructs = dataCols.map { colName =>
    struct(
      lit(colName).alias("Column"),
      col(s"df1.$colName").alias("Old_Value"),
      col(s"df2.$colName").alias("New_Value")
    ).alias(colName)
  }

  // Add all differences as an array of structs
  val dfWithDiffs = dfJoined.withColumn("Diffs", array(diffStructs: _*))
    .withColumn("Diffs", expr("filter(Diffs, d -> d.Old_Value != d.New_Value OR d.Old_Value IS NULL OR d.New_Value IS NULL)"))

  // Explode differences into multiple rows (only mismatched values)
  val dfExploded = dfWithDiffs
    .withColumn("Difference", explode(col("Diffs")))
    .select(
      col(joinCols.head), // Keeping only first join column in output (can extend if needed)
      col("Difference.Column").alias("Column"),
      col("Difference.Old_Value").alias("Old_Value"),
      col("Difference.New_Value").alias("New_Value")
    )

  dfExploded // Return the DataFrame
}
