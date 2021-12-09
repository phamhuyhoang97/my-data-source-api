package org.hoangph8.spark.dsapi.text.v2

import com.github.tototoshi.csv.CSVReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

import java.io.File

class CsvPartitionReaderV2(inputPartition: CsvPartitionV2) extends PartitionReader[InternalRow]{
  var iterator: Iterator[Seq[String]] = null
  val reader = CSVReader.open(new File(inputPartition.path))

  override def next(): Boolean = {
    if (iterator == null) {
      iterator = reader.iterator
      if (inputPartition.header) {
        iterator.next
        iterator.next
      } else iterator.next
    }
    iterator.hasNext
  }

  override def get(): InternalRow = {
    val line = iterator.next()
    InternalRow.fromSeq(line.map(value => UTF8String.fromString(value)))
  }

  override def close(): Unit = {
    try{
      if (reader != null) {
        reader.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
