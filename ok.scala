// Extract the first row (assuming fields are in the last column)
val fieldsRow = df.select("fields").as[Seq[String]].head

// Create a list of current column names (_c0, _c1, ...)
val oldColumns = df.columns.filter(_.startsWith("_c"))

// Map old column names to new ones from fields
val renamedCols = oldColumns.zip(fieldsRow).map {
  case (oldName, newName) => col(oldName).as(newName)
}

// Apply renaming
val dfRenamed = df.select(renamedCols: _*)
