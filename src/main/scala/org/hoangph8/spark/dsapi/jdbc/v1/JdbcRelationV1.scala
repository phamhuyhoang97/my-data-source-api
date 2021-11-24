package org.hoangph8.spark.dsapi.jdbc.v1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class JdbcRelationV1(
    override val sqlContext: SQLContext,
    url: String,
    user: String,
    password: String,
    table: String,
    userschema: StructType
  ) extends BaseRelation with TableScan{
  override def schema: StructType = {
    if (userschema != null) {
      // The user defined a schema, simply return it
      userschema
    } else {
      // There is no user-defined schema.
      // You need to infer it on your own. E.g., read the header of CSV file.
      throw new IllegalArgumentException("Schema must be required!")
    }
  }

  override def buildScan(): RDD[Row] = {
    new JdbcRDD(sqlContext, url, user, password, table)
  }
}
