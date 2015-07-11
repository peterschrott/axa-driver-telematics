package com.peedeex21.axa

import com.peedeex21.axa.io.AxaInputFormat2
import com.peedeex21.axa.model.Drive
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
    val axaDS = env.readFile(new AxaInputFormat2(), inputPath)

    val drives = axaDS.filter(_.seqNo != -1)
        .groupBy(d => (d.driverId, d.driveId))
        .reduceGroup(elements => {
          val list = elements.toList
          Drive(list.head.driverId, list.head.driveId, list)
        })

    /*
     * extract some nice features for each drive :)
     */
    val extractor = new FeatureExtractor()
    val enrichedDSs = extractor.extract(drives)

    /* emit the meta info*/
    enrichedDSs._1.writeAsText(outputPath + "/drive-meta/",
      writeMode = FileSystem.WriteMode.OVERWRITE)
    /* emit the enriched x- / y-coordinates */
    enrichedDSs._2.writeAsText(outputPath + "/drive-logs/",
      writeMode = FileSystem.WriteMode.OVERWRITE)

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
