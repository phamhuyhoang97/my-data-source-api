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
            throw new IllegalArgumentException("Schema must be required!")
        }
    }

    override def buildScan(): RDD[Row] = {
        val initialRdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
        val schemaFields = schema.fields

        val rowsRdd = initialRdd.map(fileContent => {
            val data = parseData(fileContent)

            val records = data.map(words => words.zipWithIndex.map {
                case (value, index) =>

                    val columnType = schemaFields(index).dataType
                    val castValue = columnType match {
                        case StringType => value
                        case IntegerType => if(value.equals("")) 0 else value.toInt
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
        val schemaFields = schema.fields

        val rowsRdd = initialRdd.map(fileContent => {
            val data = parseData(fileContent)

            val records = data.map(words => words.zipWithIndex.map {
                case (value, index) =>
                    val field = schemaFields(index)
                    if (requiredColumns.contains(field.name)) Some(Util.cast(value, field.dataType)) else None
            })

            records.map(record => Row.fromSeq(record.filter(_.isDefined).map(value => value.get)))
        })

        rowsRdd.flatMap(row => row)
    }

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
        // Nothing is actually pushed down, just iterate through all filters and print them
        println("Trying to push down filters...")
        filters foreach println
        buildScan(requiredColumns)
    }

    private def parseData(fileContent: String): Array[Seq[String]] ={
        val lines = fileContent.split("\\r?\\n")
        val data = lines.map(line => {
            val words = line.split(",").map(word => word.trim)
            Seq(words(0),words(1),words(2),words(3),words(4))
        })
        data
    }

}