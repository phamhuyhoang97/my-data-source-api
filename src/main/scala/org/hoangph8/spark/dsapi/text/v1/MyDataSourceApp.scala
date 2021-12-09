package org.hoangph8.spark.dsapi.text.v1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, upper}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.util.concurrent.TimeUnit

/**
 * @author ${user.name}
 */
object MyDataSourceApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("spark-custom-datasource")
    val spark = SparkSession.builder().config(conf).master("local[2]").getOrCreate()

    val schema = StructType(
      StructField("date", StringType, nullable = true) ::
        StructField("country", StringType, nullable = true) ::
        StructField("state", StringType, nullable = true) ::
        StructField("fips", IntegerType, nullable = true) ::
        StructField("cases", IntegerType, nullable = true) ::
        //        StructField("deaths", IntegerType, nullable = true) ::
        Nil
    )

    val df = spark
      .read
      .format("hoang")
      .schema(schema)
      .load("data/")

    // Step 1 (Schema verification)
    df.printSchema
    // Step 2 (Read data)
    df.show
    print(df.count)

    val z = df.select("state", "fips")
    z.show

    val x = z.withColumn("state", upper(col("state"))).filter("fips < 10000")
    x.show

    // Step 3 (Write data)
    //    df.write
    //      .options(Map("format" -> "customFormat"))
    //      .mode(SaveMode.Overwrite)
    //      .format("org.hoangph8.spark.ds.api")
    //      .save("out/")
    // Step 4 (Column Pruning)
    //    df.createOrReplaceTempView("covid19")
    //    spark.sql("SELECT state, fips FROM covid19").show
    //    // Step 5 (Filter Pushdown)
    //    val filterDF = spark.sql("SELECT state, fips FROM covid19 WHERE fips > 10000")
    //    filterDF.show
    //    print(filterDF.count)

    println("Ket thuc roi do")
    Thread.sleep(TimeUnit.DAYS.toMillis(1))
  }

}
