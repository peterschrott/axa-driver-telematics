package com.peedeex21.axa

import com.peedeex21.axa.model.{DriveFeatures, DriveMeta_OD}
import hex.deeplearning.{DeepLearning, DeepLearningParameters}
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import water.Key
import water.app.SparkContextSupport;

case class DrivesWReconErrOpt(driverId: Option[Double], driveId: Option[Double],
                              reconErr: Option[Double])

case class DrivesWReconErr(driverId: Int, driveId: Int, reconErr: Double)

case class KaggleResult(driverId: Int, driveId: Int, outlier: Boolean) {

  /*
   * e.g.
   * driver_trip,prob
   * 1_1,1
   */
  override def toString = {
    val prob = if (outlier) "0" else "1"
    driverId + "_" + driveId + "," + prob
  }
}


object OutlierDetection extends SparkContextSupport {

  implicit class PowerInt(val i: Double) extends AnyVal {
    def **(exp: Double): Double = math.pow(i, exp)
  }

  private var inputPath: String = null
  private var featureVariant: String = "int"
  private var outputPath: String = null

  private var stdWeight: Double = 1.0
  private var l1Norm: Double = 0.0
  private var l2Norm: Double = 0.0

  def main(args: Array[String]): Unit = {

    if (!parseParameters(args)) {
      return
    }

    // Configure this application
    val conf: SparkConf = configure(
      """Sparkling Water Outlier Detection using AutoEncoder
        |on AXA Driver Telematics data set""".stripMargin)

    // Create SparkContext to execute application on Spark cluster
    val sparkCtx = new SparkContext(conf)
    implicit val h2oCtx = new H2OContext(sparkCtx).start()
    import h2oCtx._

    implicit val sqlCtx = new SQLContext(sparkCtx)

    //
    // do the setup for the different input feature sets
    //
    val (fieldNames, nonFeatures, features) = {
      if (featureVariant.equals("int")) {
        (DriveMeta_OD.pojoFields, DriveMeta_OD.nonFeatures, DriveMeta_OD.features)
      } else {
        (DriveFeatures.pojoFields, DriveFeatures.nonFeatures, DriveFeatures.features)
      }
    }

    //
    // Read CSV and transform into DataFrame
    //
    println("\n====> Running DeepLearning on the input data. \n")
    val dataRdd = sparkCtx.textFile(inputPath)

    val schema = {
      StructType(fieldNames.map(name => StructField(name, DoubleType, true)))
    }
    val rowRDD = dataRdd.map(line => {
      var rowValues = Seq.empty[Double]
      for (value <- line.trim.split(",")) {
        rowValues :+= {
          if (value.trim.toLowerCase.equals("nan")) {
            Double.NaN
          } else {
            value.toDouble
          }
        }
      }
      Row.fromSeq(rowValues)
    })

    val drivesDF: H2OFrame = sqlCtx.createDataFrame(rowRDD, schema)

    //
    // Run Deep Learning
    //
    println("\n====> Running DeepLearning on the input data. \n")
    // Training data
    val train = drivesDF //sqlResult

    /* Configure Deep Learning algorithm */
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    // ignore driverId, driveId
    dlParams._ignored_columns = nonFeatures
    // unsupervised: pick any non-constant column
    dlParams._response_column = Symbol(fieldNames(2))
    // the activation function (non-linearity) to be used the neurons in the hidden layers.
    dlParams._activation = DeepLearningParameters.Activation.Tanh
    // use the autoencoder, obviously
    dlParams._autoencoder = true
    // one hidden layer with 20 neurons
    dlParams._hidden = Array(17)
    // no dropping of constant colors
    dlParams._ignore_const_cols = false
    // number of passes over the training dataset to be carried out
    dlParams._epochs = 1
    // do some regularisation
    dlParams._l1 = this.l1Norm
    dlParams._l2 = this.l2Norm


    /* build the deep learning model for the auto encoder */
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    //
    // Use model for scoring
    //
    println("\n====> Computing the reconstruction error with help of DeepLearning model. \n")
    val reconstructionErrorKey = Key.make
    val reconstructionError: H2OFrame = dlModel.scoreAutoEncoder(train, reconstructionErrorKey)
    reconstructionError.rename("Reconstruction.MSE", "reconErr")

    //
    // map the errors to the corresponding drive
    //
    println("\n====> Joining the input data with the construction errors row by row. \n")
    val result = train.deepCopy(null)
    result.remove(features)
    result.add(reconstructionError)

    val drivesWErrRdd: RDD[DrivesWReconErr] = h2oCtx.asRDD[DrivesWReconErrOpt](result)
      .map(entry =>
      DrivesWReconErr(entry.driverId.get.toInt, entry.driveId.get.toInt, entry.reconErr.get)
      )

    val driversThresholds = drivesWErrRdd.groupBy(_.driverId)
      .flatMap(group => {
      val driverId = group._1
      val reconErrors = group._2.map(_.reconErr).toSeq

      val mean = reconErrors.sum / reconErrors.length
      val std = math.sqrt(reconErrors.map(err => (err - mean) ** 2).sum / reconErrors.length)

      val threshold = mean + (std * this.stdWeight)

      Seq((driverId, threshold))
    })

    val kaggleResultRdd = drivesWErrRdd.map(entry => (entry.driverId, entry))
      .leftOuterJoin(driversThresholds)
      .map(join => {
      val driveWReconErr = join._2._1
      val threshold = join._2._2.get
      val prob = driveWReconErr.reconErr > threshold
      new KaggleResult(driveWReconErr.driverId, driveWReconErr.driveId, prob)
    })

    //
    // write the output file
    //
    println("\n====> Write the predictions to the output file. \n")
    kaggleResultRdd.saveAsTextFile(outputPath + "/kaggleResult")

    //
    // count the number of outliers per driver
    //
    val nOutlierByDriver = kaggleResultRdd.groupBy(_.driverId).flatMap(group => {
      val driverId = group._1
      val nOutlier =
        group._2.map(_.outlier match {
          case true => 1
          case false => 0
        }).toSeq.sum

      Seq((driverId, nOutlier))
    })

    nOutlierByDriver.saveAsTextFile(outputPath + "/nOutlierByDriver")

    // Stop Spark cluster and destroy all executors
    if (System.getProperty("spark.ext.h2o.preserve.executors") == null) {
      sparkCtx.stop()
    }
  }

  /**
   *
   * @param args start parameter
   * @return true if parsing of arguments was successful, otherwise false
   */
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 6) {
      inputPath = args(0)
      featureVariant = args(1)
      outputPath = args(2)
      stdWeight = args(3).toDouble
      l1Norm = args(4).toDouble
      l2Norm = args(5).toDouble
      true
    } else {
      println(
        """
          |Failed executing OutlierDetection. Wrong number of arguments.
          |  Usage: FeatureExtractor <input path> <feature type> <output path> <stdWeight> <L1> <L2>
        """.stripMargin)
      false
    }
  }

}