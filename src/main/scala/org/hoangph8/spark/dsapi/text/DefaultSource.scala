package org.hoangph8.spark.dsapi.text

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends DataSourceRegister with RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

    // relation cho read
    override def createRelation(
        sqlContext: SQLContext,
        parameters: Map[String, String],
        schema: StructType
    ): BaseRelation = {
        val pathParameter = parameters.get("path")
        pathParameter match {
            case Some(path) => new CustomDatasourceRelation(sqlContext, path, schema)
            case None => throw new IllegalArgumentException("The path parameter cannot be empty!")
        }
    }

    // relation cho write
    override def createRelation(
        sqlContext: SQLContext,
        mode: SaveMode,
        parameters: Map[String, String],
        data: DataFrame
    ): BaseRelation = {
        val pathParameter = parameters.getOrElse("path", "./output/")
        val fsPath = new Path(pathParameter)
        val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

        mode match {
            case SaveMode.Append => sys.error("Append mode is not supported by " + this.getClass.getCanonicalName); sys.exit(1)
            case SaveMode.Overwrite => fs.delete(fsPath, true)
            case SaveMode.ErrorIfExists if fs.exists(fsPath) => sys.error("Given path: " + pathParameter + " already exists!!"); sys.exit(1)
            case SaveMode.ErrorIfExists => sys.error("Given path: " + pathParameter + " already exists!!"); sys.exit(1)
            case SaveMode.Ignore => sys.exit()
        }

        val formatName = parameters.getOrElse("format", "customFormat")
        formatName match {
            case "customFormat" => saveAsCustomFormat(data, pathParameter, mode)
            case _ => throw new IllegalArgumentException(formatName + " is not supported!")
        }
        createRelation(sqlContext, parameters, data.schema)
    }

    private def saveAsCustomFormat(data: DataFrame, path: String, mode: SaveMode): Unit = {
        val customFormatRDD = data.rdd.map(row => {
            row.toSeq.map(value => value.toString).mkString(";")
        })
        customFormatRDD.saveAsTextFile(path)
    }

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        val pathParameter = parameters.get("path")
        pathParameter match {
            case Some(path) => new CustomDatasourceRelation(sqlContext, path, null)
            case None => throw new IllegalArgumentException("The path parameter cannot be empty!")
        }
    }

    override def shortName(): String = "hoang"
}
