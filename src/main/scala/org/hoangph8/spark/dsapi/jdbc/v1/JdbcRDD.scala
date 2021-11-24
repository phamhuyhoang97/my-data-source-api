package org.hoangph8.spark.dsapi.jdbc.v1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}

import java.sql.DriverManager

class JdbcRDD(
    sqlContext: SQLContext,
    url: String,
    user: String,
    password: String,
    table: String,
    columns: Array[String],
    filters: Array[Filter]) extends RDD[Row](sqlContext.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val conn = DriverManager.getConnection(url, user, password)
    val stmt = conn.prepareStatement(s"SELECT * FROM $table")
    val rs = stmt.executeQuery()

    val sqlBuilder = new StringBuilder()
    sqlBuilder.append(s"SELECT ${columns.mkString(", ")} FROM $table")

    val wheres = filters.flatMap {
      case EqualTo(attribute, value) => Some(s"$attribute = '$value'")
      case _ => None
    }
    if (wheres.nonEmpty) {
      sqlBuilder.append(s" WHERE ${wheres.mkString(" AND ")}")
    }

    val sql = sqlBuilder.toString

    context.addTaskCompletionListener(_ => conn.close())

    new Iterator[Row] {
      def hasNext: Boolean = rs.next()
      def next: Row = {
        val values = columns.map {
          case "id" => rs.getInt("id")
          case "emp_name" => rs.getString("emp_name")
          case "dep_name" => rs.getString("dep_name")
          case "salary" => rs.getBigDecimal("salary")
          case "age" => rs.getBigDecimal("age")
        }
        Row.fromSeq(values)
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(JdbcPartition(0))
}

case class JdbcPartition(idx: Int) extends Partition {
  override def index: Int = idx
}