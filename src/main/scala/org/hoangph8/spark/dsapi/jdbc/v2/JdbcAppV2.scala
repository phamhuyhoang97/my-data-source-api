package org.hoangph8.spark.dsapi.jdbc.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}

object JdbcAppV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("tutorial_id", IntegerType),
      StructField("tutorial_title", StringType),
      StructField("tutorial_author", StringType),
      StructField("submission_date", StringType)
      ))

    val df = spark.read
      .format("jdbc-ds-v2")
      .option("url", "jdbc:mysql://localhost:3306/test?protocol=tcp")
      .option("user", "root")
      .option("password", "my-secret-pw")
      .option("table", "tutorials_tbl")
      .load()

    df.printSchema()

    df.show
  }
}
