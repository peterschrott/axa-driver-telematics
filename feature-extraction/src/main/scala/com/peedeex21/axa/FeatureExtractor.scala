package com.peedeex21.axa

import org.apache.flink.api.common.functions.RichMapFunction
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

    val distance = data.map(x => {

      });

    // emit result
    distance.print()

    // execute programdist
    env.execute("Axa Demo")
  }

  final class FileToRowMapper extends RichMapFunction[(Double, Double), (Double, Double, String)] {

    def map(row: (Double, Double)): (Double, Double, String) = {
      (1,1,"")
    }

  }


  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      inputPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.out.println("Executing AxaDemo.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: AxaDemo <input path> <result path>")
      false
    }
  }

  private var inputPath: String = null
  private var outputPath: String = null

}
