package com.oliviernicol.sparkmore.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StructField}

object RichDataFrame {
    implicit def dfToRichDf(df : DataFrame): RichDataFrame = new RichDataFrame(df)
}

class RichDataFrame(df : Dataset[Row]) extends
    Dataset[Row](df.sparkSession, df.queryExecution.logical, RowEncoder(df.schema)) {

    def createNewColumnNameWithPrefix(prefix : String): String = {
        val columns = df.columns.toSet
        var result = prefix
        var index = 2
        while(columns.contains(result)) {
            result = s"${prefix}_$index"
            index += 1
        }
        result
    }

    def zipWithIndex(name : String, from : Long = 0) : DataFrame = {
        val rdd = df.rdd.zipWithIndex
            .map{ case (row, i) => Row.fromSeq(row.toSeq :+ (i + from)) }
        val newSchema = df.schema.add(StructField(name, LongType, false))
        df.sparkSession.createDataFrame(rdd, newSchema)
    }

    def lag(column : String, output : String = null) : DataFrame = {
        // Adding the lag column at the end of the schema with the same type as the input column
        // It is nullable because thest element of the lag column is null ;-)
        val resultColumn = if(output == null) s"${column}_lag" else output
        val resultType = df.schema(column).dataType
        val newSchema = df.schema.add(StructField(resultColumn, resultType, nullable = true))

        // last values of each partition, sorted by partition index
        val lastValues = df
            .rdd
            .map(_.getAs[Any](0))
            .mapPartitionsWithIndex{ case (index, rows) =>
                var last : Any = null
                while(rows.hasNext) last = rows.next()
                Iterator(index -> last)
            }
            .filter( _._2 != null)
            .collect().sortBy(_._1)

        // previous value of the first element of each partition
        val firstLag = lastValues
            .zipWithIndex
            .tail // avoiding index 0
            .map{ case ((partition, _), index) =>
            partition -> lastValues(index - 1)._2
        }.toMap

        // The magic happens
        val rdd : RDD[Row] = df
            .rdd
            .mapPartitionsWithIndex{ case (index, rows) =>
                var previous = firstLag.getOrElse(index, null)
                rows.map{ row =>
                    val lag = previous
                    previous = row.getAs[Any](column)
                    Row.fromSeq(row.toSeq :+ lag)
                }
            }
        df.sparkSession.createDataFrame(rdd, newSchema)
    }

    def differentiate(column : String, output : String = null) = {
        val resultColumn = if(output == null) s"${column}_diff" else output
        this.lag(column, resultColumn)
            .withColumn(resultColumn, col(column) - col(resultColumn))
    }

}
