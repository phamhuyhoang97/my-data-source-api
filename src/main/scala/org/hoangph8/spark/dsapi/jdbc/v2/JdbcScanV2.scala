package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType

class JdbcScanV2(schema: StructType, user: String, pass: String, url: String, table: String) extends Scan {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = {
    new JdbcBatchV2(user, pass, url, table)
  }
}
