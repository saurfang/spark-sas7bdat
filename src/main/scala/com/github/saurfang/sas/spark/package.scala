package com.github.saurfang.sas

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SQLContext

package object spark {

  /**
   * Adds a method, `sasFile`, to SQLContext that allows reading SAS data.
   */
  implicit class SasContext(sqlContext: SQLContext) {
    def sasFile(filePath: String,
                conf: JobConf = new JobConf()) = {
      val sasRelation = SasRelation(
        location = filePath,
        conf = conf)(sqlContext)
      sqlContext.baseRelationToDataFrame(sasRelation)
    }
  }

}
