package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.SparkSession

object JdbcAppV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val df = spark.read
      .format("jdbc-ds-v2")
      .option("url", "jdbc:mysql://localhost:3306/test?protocol=tcp")
      .option("user", "root")
      .option("pass", "my-secret-pw")
      .option("table", "employee")
      .load()

    df.printSchema
    df.show()


  }
}
