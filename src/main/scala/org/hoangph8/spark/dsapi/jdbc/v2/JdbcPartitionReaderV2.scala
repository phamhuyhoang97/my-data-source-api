package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import java.sql.{DriverManager, ResultSet}

class JdbcPartitionReaderV2(inputPartition: JdbcInputPartitionV2) extends PartitionReader[InternalRow]{
  val sqlBuilder = s"SELECT * FROM ${inputPartition.table}"
  val conn = DriverManager.getConnection(inputPartition.url, inputPartition.user, inputPartition.pass)
  val stmt = conn.prepareStatement(sqlBuilder, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
  stmt.setFetchSize(1000)
  val rs = stmt.executeQuery()

  override def next(): Boolean = {
    rs.next()
  }

  override def get(): InternalRow = {
    InternalRow.fromSeq(Seq(rs.getInt("tutorial_id"), rs.getString("tutorial_title"), rs.getString("tutorial_author"), rs.getString("submission_date")))
  }

  override def close(): Unit = {
    conn.close()
    stmt.close()
    rs.close()
  }
}
