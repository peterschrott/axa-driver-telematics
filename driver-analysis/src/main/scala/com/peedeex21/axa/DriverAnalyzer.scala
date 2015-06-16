package com.peedeex21.axa

import com.peedeex21.axa.algorithm.DuplicateLocations
import com.peedeex21.axa.io.AxaInputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem

/**
 * Created by Peter Schrott on 15.06.15.
 */
object DriverAnalyzer {

  private var inputPath: String = null
  private var outputPath: String = null

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    /* set up the execution environment */
    val env = ExecutionEnvironment.getExecutionEnvironment

    /* get raw input data */
    val driveDS = env.readFile(new AxaInputFormat(), inputPath).setParallelism(1)

    /* extract some nice features for each drive :) */
    val extractor = new FeatureExtractor(env)
    val withFeatures = extractor.extract(driveDS)
    val driverDS = withFeatures._1
    val driveLogDS = withFeatures._2

    /* search for duplicate locations within the drive logs */
    val dubLoc = DuplicateLocations()
    dubLoc.setMinimumCount(1)
    val duplicateLocationsDS = dubLoc.transform(driveLogDS)

    /* emit the results */
    driverDS.writeAsText(outputPath + "/res-driver/", writeMode = FileSystem.WriteMode.OVERWRITE)
    driveLogDS.writeAsText(outputPath + "/res-drivelog/", writeMode = FileSystem.WriteMode.OVERWRITE)
    duplicateLocationsDS.writeAsText(outputPath + "/res-duplicate-locations/", writeMode = FileSystem.WriteMode.OVERWRITE)
    
    env.execute("AXA DriverAnalyzer")
  }

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      inputPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.out.println("Failed executing DriverAnalyzer.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: DriverAnalyzer <input path> <result path>")
      false
    }
  }

}
