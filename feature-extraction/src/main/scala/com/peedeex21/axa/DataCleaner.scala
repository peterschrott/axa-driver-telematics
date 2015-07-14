package com.peedeex21.axa


import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import scala.collection.JavaConverters._

/**
 * Created by peter on 14.07.15.
 */
object DataCleaner {

  private var inputPathDropouts: String = null
  private var inputPathDataSet: String = null
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
    val dropoutDS = env.readTextFile(inputPathDropouts)
        .map(line => {
          val lineSplit = line.replace("(", "").replace(")","").split(",")
          (lineSplit(0) + "_" + lineSplit(1))
        })

    val trainDS = env.readTextFile(inputPathDataSet)
        .map(line => {
          val splitLine = line.split(",")

          val key = splitLine(0) + "_" + splitLine(1)
          (key, line)
        })

    val filteredTrainDS = trainDS.flatMap(new DropoutFilter())
        .withBroadcastSet(dropoutDS, "dropouts").map(_._2)

    val kaggleResult = dropoutDS.map(_ + ",0")


    /* emit the result */
    filteredTrainDS.writeAsText(outputPath + "/filtered-dataset/",
        writeMode = FileSystem.WriteMode.OVERWRITE)

    kaggleResult.writeAsText(outputPath + "/kaggle-outliers/",
      writeMode = FileSystem.WriteMode.OVERWRITE)

    /* execute program distributed */
    env.execute("AXA DataCleaner")
  }

  class DropoutFilter extends RichFlatMapFunction[(String, String), (String, String)] {

    var dropOuts = List.empty[String]

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      dropOuts = getRuntimeContext.getBroadcastVariable[String]("dropouts").asScala.toList
    }

    override def flatMap(value: (String, String), out: Collector[(String, String)]): Unit = {
      if (!dropOuts.contains(value._1)) {
        out.collect(value)
      }
    }

  }

  /**
   *
   * @param args start parameter
   * @return true if parsing of arguments was successful, otherwise false
   */
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 3) {
      inputPathDropouts = args(0)
      inputPathDataSet = args(1)
      outputPath = args(2)
      true
    } else {
      System.out.println("Failed executing DataCleaner.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: FeatureExtractor <input path dropouts> <input path dataset> <result path>")
      false
    }
  }

}
