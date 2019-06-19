package com.oliviernicol.sparkmore.sql

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object RichDataFrame {
    implicit def dfToRichDf(df : DataFrame): RichDataFrame = new RichDataFrame(df)
}

class RichDataFrame(df : Dataset[Row]) extends
    Dataset[Row](df.sparkSession, df.queryExecution.logical, RowEncoder(df.schema)) {

    def zipWithIndex(name : String, from : Long = 0) : DataFrame = {
        DataFrameUtil.zipWithIndex(this, name, from)
    }

}
