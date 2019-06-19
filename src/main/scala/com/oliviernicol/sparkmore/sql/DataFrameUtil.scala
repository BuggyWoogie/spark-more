package com.oliviernicol.sparkmore.sql

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StructField}

object DataFrameUtil {

    def zipWithIndex(df : DataFrame, name : String, from : Long = 0) : DataFrame = {
        val rdd = df.rdd.zipWithIndex
            .map{ case (row, i) => Row.fromSeq(row.toSeq :+ (i + from)) }
        val newSchema = df.schema.add(StructField(name, LongType, false))
        df.sparkSession.createDataFrame(rdd, newSchema)
    }

}
