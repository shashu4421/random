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


import org.apache.spark.sql.functions._

val dfNormalized = df.withColumn(
  "UAT_Normalized",
  when(col("Column") === "legDetails",
       regexp_replace(col("UAT").cast("decimal(20,6)"), "(\\.0+)$", "") // Ensure proper type handling
  ).otherwise(col("UAT"))
).withColumn(
  "Production_Normalized",
  when(col("Column") === "legDetails",
       regexp_replace(col("Production").cast("decimal(20,6)"), "(\\.0+)$", "")
  ).otherwise(col("Production"))
)

// Compare after normalization
val dfFinal = dfNormalized.withColumn("isEqual",
  col("Production_Normalized") === col("UAT_Normalized")
)

dfFinal.show(false)


import org.apache.spark.sql.functions._

val dfNormalized = df.withColumn(
  "UAT_Normalized",
  when(col("Column") === "legDetails",
       regexp_replace(col("UAT"), "\\.000000$", "") // Remove ".000000" only when it appears at the end
  ).otherwise(col("UAT"))
).withColumn(
  "Production_Normalized",
  when(col("Column") === "legDetails",
       regexp_replace(col("Production"), "\\.000000$", "")
  ).otherwise(col("Production"))
)

// Compare after normalization
val dfFinal = dfNormalized.withColumn("isEqual",
  col("Production_Normalized") === col("UAT_Normalized")
)


dfFinal.show(false)




import org.apache.spark.sql.functions._

val removeTrailingZeros = udf((value: String) => {
  if (value == null) null
  else value.split("\\|").map(_.replaceAll("\\.000000$", "")).mkString("|")
})

val dfNormalized = df.withColumn(
  "UAT_Normalized",
  when(col("Column") === "legDetails", removeTrailingZeros(col("UAT")))
    .otherwise(col("UAT"))
).withColumn(
  "Production_Normalized",
  when(col("Column") === "legDetails", removeTrailingZeros(col("Production")))
    .otherwise(col("Production"))
)

import java.text.Normalizer

val normalizeText = udf((s: String) => {
  if (s == null) null
  else Normalizer.normalize(s.trim.replaceAll("\\s+", " "), Normalizer.Form.NFKC)
})

df.withColumn("Production_Normalized", normalizeText(col("Production")))
  .withColumn("UAT_Normalized", normalizeText(col("UAT")))
  .show(false)


// Compare after normalization
val dfFinal = dfNormalized.withColumn("isEqual",
  col("Production_Normalized") === col("UAT_Normalized")
)

dfFinal.show(false)


val toAscii = udf((s: String) => s.replaceAll("[^\\x00-\\x7F]", ""))

df.withColumn("Production_ASCII", toAscii(col("Production")))
  .withColumn("UAT_ASCII", toAscii(col("UAT")))
  .show(false)




