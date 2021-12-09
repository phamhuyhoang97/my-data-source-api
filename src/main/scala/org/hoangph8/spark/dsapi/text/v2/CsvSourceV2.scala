package org.hoangph8.spark.dsapi.text.v2

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class CsvSourceV2 extends TableProvider with DataSourceRegister{
  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()
  }

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table = {
    val path = map.get("path")
    new CsvTableBatchV2(path)
  }

  override def shortName(): String = "csv-ds-v2"
}
