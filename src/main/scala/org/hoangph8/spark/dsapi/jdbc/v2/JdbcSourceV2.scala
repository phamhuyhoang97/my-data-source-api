package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.sql.{DriverManager, ResultSet}
import java.util
import scala.collection.JavaConverters._

class JdbcSourceV2 extends TableProvider with DataSourceRegister{
  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("emp_name", StringType),
      StructField("dep_name", StringType),
      StructField("salary", DoubleType),
      StructField("age", IntegerType)
    ))
    getTable(schema, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()
  }

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table = {
    val user = map.get("user")
    val pass = map.get("pass")
    val url = map.get("url")
    val table = map.get("table")
    new JdbcTableV2(structType, user, pass, url, table )
  }

  override def shortName(): String = "jdbc-ds-v2"
}

class JdbcTableV2(schema: StructType, user: String, pass: String, url: String, table: String) extends SupportsRead{
  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    new JdbcScanBuilderV2(schema, user, pass, url, table)
  }

  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }
}

class JdbcScanBuilderV2(schema: StructType, user: String, pass: String, url: String, table: String) extends ScanBuilder {
  override def build(): Scan = new JdbcScanV2(schema, user, pass, url, table)
}

case class JdbcInputPartitionV2(val partitionNumber: Int, user: String, pass: String, url: String, table: String) extends InputPartition

class JdbcScanV2(schema: StructType, user: String, pass: String, url: String, table: String) extends Scan with Batch {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(JdbcInputPartitionV2(0, user, pass, url, table))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new JdbcPartitionReaderFactoryV2
  }
}

class JdbcPartitionReaderFactoryV2 extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    new JdbcPartitionReaderV2(inputPartition.asInstanceOf[JdbcInputPartitionV2])
  }
}

class JdbcPartitionReaderV2(inputPartition: JdbcInputPartitionV2) extends PartitionReader[InternalRow] {
  val table = inputPartition.table
  val user = inputPartition.user
  val pass = inputPartition.pass
  val url = inputPartition.url

  val sql = s"SELECT * FROM $table"
  val conn = DriverManager.getConnection(url, user, pass)
  val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  stmt.setFetchSize(1000)
  val rs = stmt.executeQuery()

  override def next(): Boolean = {
    rs.next()
  }

  override def get(): InternalRow = {
    InternalRow.fromSeq(Seq(rs.getInt("id"),
      UTF8String.fromString(rs.getString("emp_name")),
      UTF8String.fromString(rs.getString("dep_name")),
      rs.getDouble("salary"),
      rs.getInt("age")))
  }

  override def close(): Unit = {
    if(conn != null) {
      try {
        conn.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    if(stmt != null) {
      try {
        stmt.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    if(rs != null) {
      try {
        rs.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}