package org.hoangph8.spark.dsapi.jdbc.v1

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}

object JdbcDataSourceApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("emp_name", StringType),
      StructField("dep_name", StringType),
      StructField("salary", DecimalType(7, 2)),
      StructField("age", DecimalType(3, 0)
    )))

    val df = spark.read
      .format("jdbc-source-v1")
      .schema(schema)
      .option("url", "jdbc:mysql://localhost:3306/test?protocol=tcp")
      .option("user", "root")
      .option("password", "my-secret-pw")
      .option("table", "employee")
      .load()

    df.printSchema()
    df.show()
    df.explain(true)

    println("_________________________________________")

    val df_sel = df.select("emp_name")
    df_sel.show
    df_sel.explain(true)

    println("_________________________________________")

    val df2 = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test?protocol=tcp")
      .option("user", "root")
      .option("password", "my-secret-pw")
      .option("dbtable", "employee")
      .load()


    df2.printSchema()
    df2.show()
    df2.explain(true)

    println("_________________________________________")

    val df_sel2 = df2.select("emp_name")
    df_sel2.show
    df_sel2.explain(true)

//    val dfSelect = spark.sql("SELECT COUNT(*), AVG(salary) FROM employee WHERE dep_name = 'Management'")
//    dfSelect.explain(true)
//    dfSelect.show()

    TimeUnit.MINUTES.sleep(5)
  }
}
