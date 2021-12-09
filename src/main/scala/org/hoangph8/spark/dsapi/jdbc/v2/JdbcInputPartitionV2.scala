package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.connector.read.InputPartition

case class JdbcInputPartitionV2(val partitionNumber: Int, user: String, pass: String, url: String, table: String) extends InputPartition