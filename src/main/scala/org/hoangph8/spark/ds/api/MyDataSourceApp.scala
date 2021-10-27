package org.hoangph8.spark.ds.api

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author ${user.name}
 */
object MyDataSourceApp {
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("spark-custom-datasource")
    val spark = SparkSession.builder().config(conf).master("local[1]").getOrCreate()

    val df = spark.read.format("org.hoangph8.spark.ds.api").load("data/")

    // Step 1 (Schema verification)
    df.printSchema()
    // Step 2 (Read data)
    df.show()
    // Step 3 (Write data)
    df.write
      .options(Map("format" -> "customFormat"))
      .mode(SaveMode.Overwrite)
      .format("org.hoangph8.spark.ds.api")
      .save("out/")
    // Step 4 (Column Pruning)
    df.createOrReplaceTempView("salaries")
    spark.sql("SELECT surname, salary FROM salaries").show()
    // Step 5 (Filter Pushdown)
    spark.sql("SELECT name, surname FROM salaries WHERE salary > 40000").show()
  }

}
