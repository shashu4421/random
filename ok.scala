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
