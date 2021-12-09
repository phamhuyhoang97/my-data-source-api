package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

class JdbcScanBuilderV2(schema: StructType, user: String, pass: String, url: String, table: String) extends ScanBuilder {
  override def build(): Scan = new JdbcScanV2(schema, user, pass, url, table)
}
