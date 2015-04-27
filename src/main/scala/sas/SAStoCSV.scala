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

    val rowCount = sasFileReader.getSasFileProperties.getRowCount

    Iterator
      .continually(sasFileReader.readNext())
      .takeWhile(_ != null)
      .zipWithIndex
      .foreach {
      case (row, i) =>
        csvDataWriter.writeRow(sasFileReader.getColumns, row)

        if (i % 10000 == 0) {
          logInfo(s"${i * 100 / rowCount} % ($i/$rowCount)")
        }
    }

    writer.close()
    inputStream.close()
  }
}
