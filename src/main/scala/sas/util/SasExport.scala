package sas.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Export sas7bdat file to parquet/csv
 */
object SasExport extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo(args.mkString(" "))

    val sparkConf = new SparkConf()
      .setAppName("SAStoCSV")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local")
    }
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sas.spark._
    val df = sqlContext.sasFile(args(0))

    val output = args(1)
    if (output.endsWith(".csv")) {
      import com.databricks.spark.csv._
      df.saveAsCsvFile(output, Map("header" -> "true"))
    } else if (output.endsWith(".parquet")) {
      df.saveAsParquetFile(output)
    }
  }
}
