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

val dfWithDiffs = dfJoined.withColumn("Diffs", array(diffStructs: _*))
  .withColumn("Diffs", expr("filter(Diffs, d -> d.Old_Value != d.New_Value OR (d.Old_Value IS NULL AND d.New_Value IS NOT NULL) OR (d.Old_Value IS NOT NULL AND d.New_Value IS NULL))"))


import org.apache.spark.sql.functions._

val dfNormalized = df.withColumn(
  "UAT_Normalized",
  when(col("Column") === "legDetails",
       regexp_replace(col("UAT"), "(\\.0+)$", "") // Remove .000000 only for legDetails
  ).otherwise(col("UAT"))
).withColumn(
  "Production_Normalized",
  when(col("Column") === "legDetails",
       regexp_replace(col("Production"), "(\\.0+)$", "") // Remove .000000 only for legDetails
  ).otherwise(col("Production"))
)

// Compare after normalization
val dfFinal = dfNormalized.withColumn("isEqual",
  col("Production_Normalized") === col("UAT_Normalized")
)

dfFinal.show(false)
