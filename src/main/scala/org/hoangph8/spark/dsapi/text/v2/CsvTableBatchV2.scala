package org.hoangph8.spark.dsapi.text.v2

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class CsvTableBatchV2(path: String, header: Boolean=true) extends SupportsRead {
  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    new CsvScanBuilderV2(path, header)
  }

  override def name(): String = this.getClass.toString

  override def schema(): StructType = SchemaUtils.getSchema(path)

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }
}

object SchemaUtils {
  def getSchema(path:String):StructType = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val firstLine = sparkContext.textFile(path).first()
    val columnNames = firstLine.split(",")
    val structFields = columnNames.map(value => StructField(value, StringType))
    StructType(structFields)
  }
}