package org.hoangph8.spark.ds.api

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan, PrunedScan, TableScan}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

class CustomDatasourceRelation(
    override val sqlContext: SQLContext,
    path: String,
    userSchema: StructType
) extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with Serializable{
    override def schema: StructType = {
        if (userSchema != null) {
            // The user defined a schema, simply return it
            userSchema
        } else {
            // There is no user-defined schema.
            // You need to infer it on your own. E.g., read the header of CSV file.
            StructType(
                StructField("name", StringType, nullable = true) ::
                    StructField("surname", StringType, nullable = true) ::
                    StructField("salary", IntegerType, nullable = true) ::
                    Nil
            )
        }
    }

    override def buildScan(): RDD[Row] = {
        val initialRdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
        val schemaFields = schema.fields

        val rowsRdd = initialRdd.map(fileContent => {
            val lines = fileContent.split("-")
            val data = lines.map(line => line.split("\\$").toSeq)

            val records = data.map(words => words.zipWithIndex.map {
                case (value, index) =>

                    val columnType = schemaFields(index).dataType
                    val castValue = columnType match {
                        case StringType => value
                        case IntegerType => value.toInt
                    }
                    castValue
            })
            records.map(record => Row.fromSeq(record))
        })

        rowsRdd.flatMap(row => row)
    }

    override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
        println("Selecting only required columns...")
        // An example, does not provide any specific performance benefits
        val initialRdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
        println("DMMMMMMMMMMMMMMMMMMMMMMMMMM")
        initialRdd.collect().foreach(o => {
            val z = o.getBytes()
            z.foreach(println)
            println("++++++++++++++++++++++++++++++")
        })
        val schemaFields = schema.fields

        val rowsRdd = initialRdd.map(fileContent => {
            println("CCCCCCCCCCCCCCCCCCCCC")
            val lines = fileContent.split("-")
            lines.foreach(o => println(o))
            val data = lines.map(line => line.split("\\$").toSeq)
            data.foreach(o => println(o))

            val records = data.map(words => words.zipWithIndex.map {
                case (value, index) =>
                    val field = schemaFields(index)
                    if (requiredColumns.contains(field.name)) Some(cast(value, field.dataType)) else None
            })

            records
                .map(record => record.filter(_.isDefined))
                .map(record => Row.fromSeq(record))
        })

        rowsRdd.flatMap(row => row)
    }

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
        // Nothing is actually pushed down, just iterate through all filters and print them
        println("Trying to push down filters...")
        filters foreach println
        buildScan(requiredColumns)
    }

    private def cast(value: String, dataType: DataType) = dataType match {
        case StringType => value
        case IntegerType => value.toInt
    }

}