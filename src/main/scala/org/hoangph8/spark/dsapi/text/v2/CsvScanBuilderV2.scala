package org.hoangph8.spark.dsapi.text.v2

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class CsvScanBuilderV2(path: String, header: Boolean=true) extends ScanBuilder {
  override def build(): Scan = new CsvScanV2(path, header)
}
