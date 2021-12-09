package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class JdbcSourceV2 extends TableProvider with DataSourceRegister{
  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()
  }

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table = {
    val user = map.get("user")
    val pass = map.get("pass")
    val url = map.get("url")
    val table = map.get("table")
    new JdbcTableV2(structType, user, pass, url, table)
  }

  override def shortName(): String = "jdbc-ds-v2"
}
