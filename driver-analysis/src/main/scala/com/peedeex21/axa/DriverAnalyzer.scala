package com.peedeex21.axa

import com.peedeex21.axa.algorithm.DuplicateLocations
import com.peedeex21.axa.io.AxaInputFormat
import com.peedeex21.axa.model.DriveMeta
import org.apache.flink.api.scala._
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

  private val driverIds = Seq(1, 2)

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    /* set up the execution environment */
    val env = ExecutionEnvironment.getExecutionEnvironment

    val (driveMetaDS, driveLogDS) = {
      if(extractFeatures) {
        /* get raw input data */
        val axaDS = env.readFile(new AxaInputFormat(), inputPath).setParallelism(1)
        /* extract some nice features for each drive :) */
        val extractor = new FeatureExtractor()
        extractor.extract(axaDS)
      } else {
        val driveMetaDS = env.readCsvFile[DriveMeta](inputPath + "/drive-meta/",
          pojoFields=DriveMeta.pojoFields)
        //driveLogDS = Some(env.readCsvFile[DriveLog](inputPath + "/drive-logs/"))
        (driveMetaDS, null)
      }
    }

    /* search for duplicate locations within the drive logs */
    val dubLoc = DuplicateLocations()
    dubLoc.setMinimumCount(1)
    val duplicateLocationsDS = dubLoc.transform(driveLogDS)

    /* split the data set into training and test data */
    driveMetaDS.groupBy(drive => (drive.driverId))

    /* get drives of current driver as positives & drives of other drivers as negatives */
    val drivesThis = driveMetaDS.filter(_.driverId == 1)
      //.map(d => (d, rand.nextDouble()))
      //.sortPartition(1, Order.ASCENDING)

    val drivesOthers = driveMetaDS.filter(_.driverId != 1)
      //.map(d => (d, rand.nextDouble()))
      //.sortPartition(1, Order.ASCENDING)

    /* assemble the training data set */
    val thisDS_Tr = drivesThis
      .filter(d => d.driveId % 4 != 0)
      .map(entry => new LabeledVector(1.0, entry.toFeatureVector))

    val otherDS_Tr = drivesOthers
      //.first(100)
      .filter(d => d.driveId % 4 != 0)
      .map(entry => new LabeledVector(-1.0, entry.toFeatureVector))

    val targetDS_Tr = thisDS_Tr.union(otherDS_Tr)

    /* assemble the test data set */
    val thisDS_Te = drivesThis
      //.filter(entry => entry._2 > 0.85)
      .filter(d => d.driveId % 4 == 0)
      .map(entry => new LabeledVector(1.0, entry.toFeatureVector))

    val othersDS_Te = drivesOthers
      //.first(30)
      .filter(d => d.driveId % 4 == 0)
      .map(entry => new LabeledVector(-1.0, entry.toFeatureVector))

    val targetDS_Te = thisDS_Te.union(othersDS_Te)

    /* do the smv */
    val svm = SVM()
    svm.setRegularization(0.8)
    svm.setStepsize(0.3)
    svm.fit(targetDS_Tr)
    val svmPredictions = svm.predict(targetDS_Te)
    svmPredictions.print()

    val evaluation = svmPredictions.map(result => (result._1, math.signum(result._2)))
        .map(result => (result._1 == result._2, 1))
        .groupBy(_._1)
        .reduce((l, r) => (l._1, l._2 + r._2))

    /* emit the results*/
    if(extractFeatures) {
      driveMetaDS.writeAsText(outputPath + "/drive-meta/", writeMode = FileSystem.WriteMode.OVERWRITE)
      driveLogDS.writeAsText(outputPath + "/drive-log/", writeMode = FileSystem.WriteMode.OVERWRITE)
    }
    duplicateLocationsDS.writeAsText(outputPath + "/res-duplicate-locations/", writeMode = FileSystem.WriteMode.OVERWRITE)
    evaluation.writeAsText(outputPath + "/res-smv/", writeMode = FileSystem.WriteMode.OVERWRITE)
    
    //env.execute("AXA DriverAnalyzer")
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
