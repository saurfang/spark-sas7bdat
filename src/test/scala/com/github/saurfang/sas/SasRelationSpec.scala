package com.github.saurfang.sas

import com.ggasoftware.parso.SasFileConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SharedSparkContext}
import org.scalactic.TolerantNumerics
import org.scalatest._
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

class SasRelationSpec extends FlatSpec with Matchers with SharedSparkContext{
  val BLOCK_SIZE = 3 * 1024 * 1024

  "SASReltion" should "read SAS data exactly correct" in {
    val spark = SparkSession.builder().getOrCreate()

    val randomDF = spark.read.format("com.github.saurfang.sas.spark").load(getClass.getResource("/random.sas7bdat").getPath).cache()
    randomDF.printSchema()

    randomDF.count() should ===(1000000)

    val referenceDF = spark.read.format("csv").option("header", "true").load(getClass.getResource("/random.csv").getPath).cache()

    import TolerantNumerics._
    implicit val dblEquality = tolerantDoubleEquality(SasFileConstants.EPSILON)

    randomDF.collect().zip(referenceDF.collect()).foreach {
      case (row1, row2) =>
        row1.getDouble(0) should ===(row2.getString(0).toDouble)
        row1.getDouble(1).toLong should ===(row2.getString(1).toLong)
    }

  }

  "SASRelation" should "support export datetime to csv/parquet" in {
    val spark = SparkSession.builder().getOrCreate()

    val dtDF = spark.read.format("com.github.saurfang.sas.spark").load(getClass.getResource("/datetime.sas7bdat").getPath).cache()
    dtDF.printSchema()

    dtDF.write.mode("overwrite").format("parquet").save(getClass.getResource("/").getPath + "datetime.parquet")
    dtDF.write.mode("overwrite").format("csv").option("header", "true").save(getClass.getResource("/").getPath + "datetime.csv")
  }
}
