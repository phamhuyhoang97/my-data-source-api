package org.hoangph8.spark.dsapi.text.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.concurrent.TimeUnit

object CsvCustomV2App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("hihi").getOrCreate()

    val simpleDF = spark.read.format("csv").option("header", "true").load("data/adult.csv")
    simpleDF.printSchema()
    simpleDF.show()

    simpleDF.groupBy("workclass").agg(sum("hours_per_week")).show

    TimeUnit.MINUTES.sleep(10)

  }
}
