package com.peedeex21.axa

import org.apache.flink.api.scala._

object FeatureExtractor {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val data: DataSet[(Double, Double)] = env.readCsvFile(inputPath)

    //TODO extract some nice features :)

    // emit result
    data.print()

    // execute programdist
    env.execute("AXA FeatureExtractor")
  }

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

  private var inputPath: String = null
  private var outputPath: String = null

}
