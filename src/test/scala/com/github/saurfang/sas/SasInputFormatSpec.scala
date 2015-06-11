package com.github.saurfang.sas

import com.github.saurfang.sas.mapred.SasInputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.spark.rdd.{HadoopRDD, NewHadoopRDD}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest._
import com.github.saurfang.sas.mapred.SasInputFormat

class SasInputFormatSpec extends FlatSpec with Matchers with Logging {
  val BLOCK_SIZE = 3 * 1024 * 1024

  "SASInputFormat" should "read correct number of observations" in {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("SASInputFormat"))
    val jobConf = new JobConf()
    jobConf.setInt("fs.local.block.size", BLOCK_SIZE)
    jobConf.setInt("mapred.min.split.size", BLOCK_SIZE)
    val path = new Path(getClass.getResource("/random.sas7bdat").getPath)
    FileInputFormat.setInputPaths(jobConf, path)

    val fileStatus = FileSystem.get(jobConf).getFileStatus(path)
    logInfo(s"Block Size: ${fileStatus.getBlockSize}")

    val sasRDD = new HadoopRDD[NullWritable, Array[Object]](
      sc,
      jobConf,
      classOf[SasInputFormat],
      classOf[NullWritable],
      classOf[Array[Object]],
      100
    )

    sasRDD.count() should ===(1000000)

    sc.stop()
  }
}
