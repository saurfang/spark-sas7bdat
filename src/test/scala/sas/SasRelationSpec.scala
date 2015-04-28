package sas

import com.ggasoftware.parso.SasFileConstants
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalactic.TolerantNumerics
import org.scalatest._

class SasRelationSpec extends FlatSpec with Matchers with Logging {
  val BLOCK_SIZE = 3 * 1024 * 1024

  "SASReltion" should "read SAS data exactly correct" in {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("SASRelation").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"))
    val sqlContext = new SQLContext(sc)

    val job = Job.getInstance
    val jobConf = job.getConfiguration
    jobConf.setInt("fs.local.block.size", BLOCK_SIZE)
    FileInputFormat.setMinInputSplitSize(job, BLOCK_SIZE)

    import sas.spark._
    val randomDF = sqlContext.sasFile(getClass.getResource("/random.sas7bdat").getPath, job).cache()
    randomDF.printSchema()

    randomDF.count() should ===(1000000)

    import com.databricks.spark.csv._
    val referenceDF = sqlContext.csvFile(getClass.getResource("/random.csv").getPath).cache()

    import TolerantNumerics._
    implicit val dblEquality = tolerantDoubleEquality(SasFileConstants.EPSILON)

    randomDF.collect().sortBy(_.getDouble(0)).zip(referenceDF.collect().sortBy(_.getString(0).toDouble)).foreach {
      case (row1, row2) =>
        row1.getDouble(0) should ===(row2.getString(0).toDouble)
        row1.getDouble(1).toLong should ===(row2.getString(1).toLong)
    }

    sc.stop()
  }
}
