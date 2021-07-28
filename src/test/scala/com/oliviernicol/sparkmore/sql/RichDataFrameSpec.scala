package com.oliviernicol.sparkmore.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{when, lit}
import org.scalatest.{FlatSpec, Matchers}
import RichDataFrame._
import com.holdenkarau.spark.testing.DataFrameSuiteBase

class RichDataFrameSpec  extends FlatSpec with Matchers with DataFrameSuiteBase  {

    import spark.implicits._

    "zipWithIndex" should "zip a column of a dataframe after implicit casting" in {
        val df : DataFrame = spark.range(100).withColumn("x", lit("8"))
        val result = df.zipWithIndex("zip")
        val expected = df.withColumn("zip", 'id)
        assertDataFrameEquals(expected, result)
    }

    it should "zip starting from a positive index" in {
        val df : DataFrame = spark.range(100).withColumn("x", lit("8"))
        val result = df.zipWithIndex("zip", from = 101010)
        val expected = df.withColumn("zip", 'id + 101010 )
        assertDataFrameEquals(expected, result)
    }

    it should "zip starting from a negative index" in {
        val df : DataFrame = spark.range(100).withColumn("x", lit("8"))
        val result = df.zipWithIndex("zip", from = -9)
        val expected = df.withColumn("zip", 'id - 9 )
        assertDataFrameEquals(expected, result)
    }

    "lag" should "work" in {
        val df : DataFrame = spark.range(10).withColumn("x", lit("8"))
        val result = df.lag("id", "id_lag").lag("x", "x_lag")
        val expected = df
            .withColumn("id_lag", when('id === 0, null).otherwise('id - 1))
            .withColumn("x_lag", when('id === 0, null).otherwise(lit("8")))
        result.show()
        result.printSchema()
        expected.show()
        expected.printSchema
        assertDataFrameEquals(expected, result)
    }

}
