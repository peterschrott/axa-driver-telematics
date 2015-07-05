package com.peedeex21.axa

import com.peedeex21.axa.algorithm.DuplicateLocations
import com.peedeex21.axa.io.AxaInputFormat
import com.peedeex21.axa.model.{DriveLog, DriveMeta}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector

import scala.util.Random

/**
 * Created by Peter Schrott on 15.06.15.
 */
object DriverAnalyzer {

  private val extractFeatures = false
  private var inputPath: String = null
  private var outputPath: String = null

  private val rand = new Random()

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    /* set up the execution environment */
    val env = ExecutionEnvironment.getExecutionEnvironment

    var driveMetaDS: Option[DataSet[DriveMeta]] = None
    var driveLogDS: Option[DataSet[DriveLog]] = None

    if(extractFeatures) {
      /* get raw input data */
      val axaDS = env.readFile(new AxaInputFormat(), inputPath).setParallelism(1)
      /* extract some nice features for each drive :) */
      val extractor = new FeatureExtractor()
      val withFeatures = extractor.extract(axaDS)
      driveMetaDS = Some(withFeatures._1)
      driveLogDS = Some(withFeatures._2)
    } else {
      driveMetaDS = Some(env.readCsvFile[DriveMeta](inputPath + "/res-drive-meta/"))
      driveLogDS = Some(env.readCsvFile[DriveLog](inputPath + "/res-drive-logs/"))
    }

    /* search for duplicate locations within the drive logs */
    val dubLoc = DuplicateLocations()
    dubLoc.setMinimumCount(1)
    val duplicateLocationsDS = dubLoc.transform(driveLogDS.get)

    /* split the data set into training and test data */

    driveMetaDS.get.groupBy(drive => (drive.driverId))

    val drivesThis = driveMetaDS.get.filter(_.driverId == 1)
      .map(d => (d, rand.nextDouble()))

    val labeledDrThis_Train = drivesThis.filter(entry => entry._2 <= 0.85)
      .map(entry => new LabeledVector(1.0, entry._1.toFeatureVector))

    val labeledDrThis_Test = drivesThis.filter(entry => entry._2 > 0.85)
      .map(entry => new LabeledVector(1.0, entry._1.toFeatureVector))

    val drivesOthers = driveMetaDS.get.filter(_.driverId != 1)
      .map(d => (d, rand.nextDouble()))
      .sortPartition(1, Order.ASCENDING)

    val labeledDrOthers_Train = drivesOthers
      .first(170)
      .map(entry => new LabeledVector(0.0, entry._1.toFeatureVector))

    val labeledDrOthers_Test = drivesOthers
      .first(30)
      .map(entry => new LabeledVector(0.0, entry._1.toFeatureVector))

    val svmDS_Train = labeledDrThis_Train.union(labeledDrOthers_Train)
    val svmDS_Test = labeledDrThis_Test.union(labeledDrOthers_Test)

    val svm = SVM()
    svm.fit(svmDS_Train)
    val svmResults = svm.predict(svmDS_Test)

    svmResults.map(result => (result._1 == result._2, 1))
        .groupBy(_._1)
        .reduce((l, r) => (l._1, l._2 + r._2))
        .print()

    /* emit the results */
    if(extractFeatures) {
      driveMetaDS.get.writeAsText(outputPath + "/res-drive-meta/", writeMode = FileSystem.WriteMode.OVERWRITE)
      driveLogDS.get.writeAsText(outputPath + "/res-drive-log/", writeMode = FileSystem.WriteMode.OVERWRITE)
    }
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
