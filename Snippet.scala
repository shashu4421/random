import org.apache.spark.sql.functions._

val df_normalized = df.withColumn(
  "normalized_value",
  concat_ws("|", array_sort(
    expr("transform(split(your_column, '\\|'), x -> concat(x, '.000000'))")
  ))
)

df_normalized.show(false)
