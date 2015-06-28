package com.github.saurfang.sas.spark

import org.apache.spark.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSuite, Matchers}

class DefaultSourceSuite extends FunSuite with Matchers with SharedSparkContext {
  test("Load data via SQL should be the same as via Spark") {
    val datetimeFile = getClass.getResource("/datetime.sas7bdat").getPath

    val sqlContext = new SQLContext(sc)

    import com.github.saurfang.sas.spark._
    val dtDF = sqlContext.sasFile(datetimeFile)

    import sqlContext._
    sql(
      s"""
        |CREATE TEMPORARY TABLE dtTable
        |USING com.github.saurfang.sas.spark
        |OPTIONS (path "$datetimeFile")
      """.stripMargin)
    val dtSQLDF = sql("SELECT * FROM dtTable")

    dtDF.collect() should ===(dtSQLDF.collect())
  }

  test("Load data via SQLContext should be the same as via Spark") {
    val datetimeFile = getClass.getResource("/datetime.sas7bdat").getPath

    val sqlContext = new SQLContext(sc)

    import com.github.saurfang.sas.spark._
    val implicitDF = sqlContext.sasFile(datetimeFile)
    val directDF = sqlContext.load("com.github.saurfang.sas.spark", Map("path" -> datetimeFile))

    implicitDF.collect() should ===(directDF.collect())
  }
}
