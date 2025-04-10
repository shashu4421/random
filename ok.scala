import scala.collection.mutable

// Parse A into a Map[eventName -> List(fieldNames)]
val schemaMap: Map[String, List[(String, String)]] = dfA
  .collect()
  .map { row =>
    val eventName = row.getAs[String]("eventName")
    val json = row.getAs[String]("fields")
    val parsed = spark.read.json(Seq(json).toDS)
    val nameTypeList = parsed.select($"name", $"jsonDatatype").as[(String, String)].collect().toList
    (eventName, nameTypeList)
  }.toMap



// Step 2: Define schema of fields column (which is a JSON array of structs)
val fieldArraySchema = ArrayType(StructType(Seq(
  StructField("name", StringType),
  StructField("jsonDatatype", StringType)
)))

// Step 3: Parse the JSON string column "fields" to Array of Structs
val dfParsed = df.withColumn("parsedFields", from_json(col("fields"), fieldArraySchema))

// Step 4: Extract just the "name" from each field
val dfFinal = dfParsed
  .withColumn("fields", expr("transform(parsedFields, x -> x.name)"))
  .select("eventName", "fields")

// Show result
dfFinal.show(false)
