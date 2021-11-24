package org.hoangph8.spark.dsapi.jdbc.v1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}

import java.sql.DriverManager

class JdbcRDD(
    sqlContext: SQLContext,
    url: String,
    user: String,
    password: String,
    table: String) extends RDD[Row](sqlContext.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val conn = DriverManager.getConnection(url, user, password)
    val stmt = conn.prepareStatement(s"SELECT * FROM $table")
    val rs = stmt.executeQuery()

    new Iterator[Row] {
      def hasNext: Boolean = rs.next()
      def next: Row = Row(rs.getInt("id"), rs.getString("emp_name"), rs.getBigDecimal("salary"), rs.getBigDecimal("age"))
    }
  }

  override protected def getPartitions: Array[Partition] = Array(JdbcPartition(0))
}

case class JdbcPartition(idx: Int) extends Partition {
  override def index: Int = idx
}