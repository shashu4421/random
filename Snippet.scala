import org.apache.spark.sql.functions._

val df_normalized = df.withColumn(
  "normalized_value",
  concat_ws("|", array_sort(
    expr("transform(split(your_column, '\\|'), x -> concat(x, '.000000'))")
  ))
)

df_normalized.show(false)


val dfWithDiffs = dfJoined.withColumn("Diffs", array(diffStructs: _*))
  .withColumn("Diffs", expr("filter(Diffs, d -> d.Old_Value != d.New_Value OR d.Old_Value IS NULL OR d.New_Value IS NULL AND NOT (d.Old_Value IS NULL AND d.New_Value IS NULL))"))

