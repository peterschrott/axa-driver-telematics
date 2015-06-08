package com.peedeex21.axa

import com.peedeex21.axa.model.AxaInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object FeatureExtractor {

  private var inputPath: String = null
  private var outputPath: String = null

  /**
   *
   * @param args start parameter
   */
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // get input data
    val input = env.readFile(new AxaInputFormat(), inputPath)

    /*
     * extract some nice features :)
     */
    val plusFeatures = input.map(_.extractAllFeatures())

    // join the features with the original data set of x and
    // as join key the driver id, drive id and sequence number is used
    val points = plusFeatures
      .flatMap(entry => {
        entry.points.map(p => {
          (entry.driveId, entry.driverId, p._1, p._2.x, p._2.y)
        })
      })

    val distances = plusFeatures
      .flatMap(entry => {
        entry.distances.map(a => {
          (entry.driveId, entry.driverId, a._1, a._2)
        })
      })

    val angles = plusFeatures
      .flatMap(entry => {
        entry.angles.map(a => {
          (entry.driveId, entry.driverId, a._1, a._2)
        })
      })

    val out = points
      .join(distances).where(0, 1, 2).equalTo(0, 1, 2)
        .map(join => (join._1._1, join._1._2, join._1._3, join._1._4, join._1._5, join._2._4))
      .join(angles).where(0, 1, 2).equalTo(0, 1, 2)
        .map(join => (join._1._1, join._1._2, join._1._3, join._1._4, join._1._5, join._1._6, join._2._4))

    // emit result to CSV file
    out.writeAsCsv(outputPath, writeMode = FileSystem.WriteMode.OVERWRITE)

    // execute program distributed
    env.execute("AXA FeatureExtractor")
  }

  /**
   *
   * @param args start parameter
   * @return true if parsing of arguments was successful, otherwise false
   */
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      inputPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.out.println("Failed executing FeatureExtractor.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: FeatureExtractor <input path> <result path>")
      false
    }
  }

}
