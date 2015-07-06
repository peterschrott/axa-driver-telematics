package com.peedeex21.axa

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object PointCount {

  val coordinatePattern = "[-+]?[0-9]*\\.?[0-9]+".r

  private var inputPath: String = null
  private var outputPath: String = null

  case class XYEntry(x: Double, y: Double) {}

  case class XYAggEntry(coordinates: XYEntry, count: Int) {}


  def main(args: Array[String]) {

    if (!parseParameters(args)) {
      return
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val readParam = new Configuration
    readParam.setBoolean("recursive.file.enumeration", true)

    // get input data
    val input: DataSet[(String, String)] = env.readCsvFile(inputPath, ignoreFirstLine = true)

    val drive = input // input type is DataSet[(Strings, String)]
      .map(entry => (
        entry._1.toDouble,
        entry._2.toDouble
      )) // transform list to DataSet[(Double, Double)]

    //plotting.plotXYScatter(drive.collect())

    val counts = drive
      .map(d => XYEntry(
        x = d._1,
        y = d._2
      ))
      .map(entry => XYAggEntry(
        coordinates = entry,
        count = 1
      )) // transform list to DataSet[XYAggEntry]
      .groupBy(agg => {
        agg.coordinates
      }) // GroupedGDataSet[XYAggEntry]
      .reduce((a, b) => XYAggEntry(
        coordinates = a.coordinates, /* forward coordinates */
        count = a.count + b.count /* aggregate partial result */
      )) // DataSet[XYAggEntry]
      .filter(agg => {
        agg.count >= 2 /* only two or more occurrences */
      }) // DataSet[XYAggEntry]
      .map(entry => (entry.coordinates.x, entry.coordinates.y)) // DataSet[(Double, Double)]

    // emit as CSV File
    //counts.writeAsCsv(outputPath, writeMode = FileSystem.WriteMode.OVERWRITE)
    //plotting.plotXYScatter(counts.collect())
    // execute program
    //env.execute("AXA PointCount")
  }

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      inputPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.out.println("Failed executing PointCount.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: PointCounter <input path> <result path>")
      false
    }
  }

}
