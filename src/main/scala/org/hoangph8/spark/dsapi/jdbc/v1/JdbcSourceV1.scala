package org.hoangph8.spark.dsapi.jdbc.v1

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class JdbcSourceV1 extends RelationProvider with DataSourceRegister with SchemaRelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new JdbcRelationV1(
      sqlContext,
      parameters.get("url").get,
      parameters.get("user").get,
      parameters.get("password").get,
      parameters.get("table").get,
      null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    new JdbcRelationV1(
      sqlContext,
      parameters.get("url").get,
      parameters.get("user").get,
      parameters.get("password").get,
      parameters.get("table").get,
      schema)
  }

  override def shortName(): String = "jdbc-source-v1"
}
