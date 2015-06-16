package com.peedeex21.axa

import com.peedeex21.axa.io.AxaInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object FeatureExtractorRunner {

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

    // get input data
    val driveDS = env.readFile(new AxaInputFormat(), inputPath).setParallelism(1)

    /*
     * extract some nice features for each drive :)
     */
    val extractor = new FeatureExtractor(env)
    val enrichedDS = extractor.extract(driveDS)

    /* emit the meta info */
    enrichedDS._1.writeAsText(outputPath + "/meta/", writeMode = FileSystem.WriteMode.OVERWRITE)
    /* emit the enriched x- / y-coordinates */
    enrichedDS._2.writeAsCsv(outputPath + "/with-features/", writeMode = FileSystem.WriteMode.OVERWRITE)

    /* execute program distributed */
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
