package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class JdbcPartitionReaderFactoryV2 extends PartitionReaderFactory{
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    new JdbcPartitionReaderV2(inputPartition.asInstanceOf[JdbcInputPartitionV2])
  }
}
