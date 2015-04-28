package sas

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SQLContext

/**
 * Created by forest on 4/27/15.
 */
package object spark {
  /**
   * Adds a method, `sasFile`, to SQLContext that allows reading SAS data.
   */
  implicit class SasContext(sqlContext: SQLContext) {
    def sasFile(filePath: String,
                job: Job = Job.getInstance()) = {
      val sasRelation = SasRelation(
        location = filePath,
        job = job)(sqlContext)
      sqlContext.baseRelationToDataFrame(sasRelation)
    }
  }
}
