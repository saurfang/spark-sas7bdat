package sas

import java.io._

import com.ggasoftware.parso.{CSVDataWriter, SasFileReader}
import org.apache.spark.Logging

/**
 * Converts sas7bdat file to csv
 */
object SAStoCSV extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo(args.mkString(" "))

    val input = new File(args(0))
    val inputStream = new BufferedInputStream(new FileInputStream(input))
    val sasFileReader = new SasFileReader(inputStream)

    val writer = new FileWriter(args(1))
    val csvDataWriter = new CSVDataWriter(writer)

    csvDataWriter.writeColumnNames(sasFileReader.getColumns)

    val nrow = sasFileReader.getSasFileProperties.getRowCount
    var rowCount = 0

    Iterator
      .continually(sasFileReader.readNext())
      .takeWhile(_ != null)
      .foreach {
      row =>
        csvDataWriter.writeRow(sasFileReader.getColumns, row)

        rowCount += 1
        if (rowCount % 10000 == 0) {
          logInfo(s"${rowCount * 100 / nrow} % ($rowCount/$nrow)")
        }
    }

    writer.close()
    inputStream.close()
  }
}
