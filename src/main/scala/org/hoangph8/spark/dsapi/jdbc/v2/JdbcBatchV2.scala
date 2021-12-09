package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class JdbcBatchV2(user: String, pass: String, url: String, table: String) extends Batch{
  override def planInputPartitions(): Array[InputPartition] = {
    Array(JdbcInputPartitionV2(0, user, pass, url, table), JdbcInputPartitionV2(1, user, pass, url, table))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new JdbcPartitionReaderFactoryV2
  }
}
