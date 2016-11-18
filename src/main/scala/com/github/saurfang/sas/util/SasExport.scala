package com.github.saurfang.sas.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.LogManager

/**
 * Export sas7bdat file to parquet/csv.
 * First argument is the input file. Second argument is the output path.
 * Output type is determined by the extension (.csv or .parquet)
 */
object SasExport {

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    log.info(args.mkString(" "))   

    val spark = SparkSession
      .builder
      .appName("Spark sas7bdat")
      .getOrCreate()

    import com.github.saurfang.sas.spark._
    val df = spark.read.format("com.github.saurfang.sas.spark").load(args(0))

    val output = args(1)
    if (output.endsWith(".csv")) {
      df.write.format("csv").option("header", "true").save(output)
    } else if (output.endsWith(".parquet")) {
      df.write.parquet(output)
    }
  }
}
