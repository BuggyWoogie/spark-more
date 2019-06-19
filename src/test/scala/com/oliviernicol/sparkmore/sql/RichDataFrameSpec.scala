package com.oliviernicol.sparkmore.sql

import org.apache.spark.sql._
import org.scalatest.{FlatSpec, Matchers}
import RichDataFrame._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class RichDataFrameSpec  extends FlatSpec with Matchers with DataFrameSuiteBase  {

    import spark.implicits._

    "A dataframe" should "be zipped after implicit casting" in {
        val df : DataFrame = spark.range(100).withColumn("x", functions.lit("8"))
        val result = df.zipWithIndex("zip")
        val expected = df.withColumn("zip", 'id)
        assertDataFrameEquals(expected, result)
    }

}
