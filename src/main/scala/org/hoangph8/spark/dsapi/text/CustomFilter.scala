package org.hoangph8.spark.dsapi.text

import org.apache.spark.sql.types.StructType

case class CustomFilter(attr : String, value : Any, filter : String)

object CustomFilter {
  def applyFilters(filters : List[CustomFilter], value : String, schema : StructType): Boolean = {
    var includeInResultSet = true

    val schemaFields = schema.fields
    val index = schema.fieldIndex(filters.head.attr)
    val dataType = schemaFields(index).dataType
    val castedValue = Util.cast(value, dataType)

    filters.foreach(f => {
      val givenValue = Util.cast(f.value.toString, dataType)
      f.filter match {
        case "equalTo" => {
          includeInResultSet = castedValue == givenValue
          println("custom equalTo filter is used!!")
        }
        case _ => throw new UnsupportedOperationException("this filter is not supported!!")
      }
    })

    includeInResultSet
  }
}
