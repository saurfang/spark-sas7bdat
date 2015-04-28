package sas.util

import java.io._

import com.ggasoftware.parso.{CSVDataWriter, SasFileReader}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext, Logging}

/**
 * Converts sas7bdat file to csv
 */
object SAStoCSV extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo(args.mkString(" "))

    val sparkConf = new SparkConf()
      .setAppName("SAStoCSV")
    if(!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local")
    }
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sas.spark._
    import com.databricks.spark.csv._
    sqlContext.sasFile(args(0)).saveAsCsvFile(args(1), Map("header" -> "true"))
  }
}
