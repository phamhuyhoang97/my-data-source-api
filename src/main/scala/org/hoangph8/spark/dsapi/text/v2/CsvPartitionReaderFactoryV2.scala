package org.hoangph8.spark.dsapi.text.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class CsvPartitionReaderFactoryV2 extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    new CsvPartitionReaderV2(inputPartition.asInstanceOf[CsvPartitionV2])
  }
}
