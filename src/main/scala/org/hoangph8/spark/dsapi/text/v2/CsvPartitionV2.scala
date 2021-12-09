package org.hoangph8.spark.dsapi.text.v2

import org.apache.spark.sql.connector.read.InputPartition

case class CsvPartitionV2(val partitionNumber: Int, path: String, header: Boolean=true) extends InputPartition
