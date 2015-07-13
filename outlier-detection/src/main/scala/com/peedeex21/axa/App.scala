package com.peedeex21.axa

import com.peedeex21.axa.model.{DriveFeatures, DriveFeatures2, DriveMeta_OD}
import hex.deeplearning.{DeepLearning, DeepLearningParameters}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import water.Key
import water.app.SparkContextSupport
import water.fvec.H2OFrame
;

case class DrivesWReconErrOpt(driverId: Option[Double], driveId: Option[Double],
                              reconErr: Option[Double])

case class DrivesWReconErr(driverId: Int, driveId: Int, reconErr: Double) {
  override def toString = {
    driverId + "," + driveId + "," + reconErr
  }
}

case class KaggleResult(driverId: Int, driveId: Int, prob: Double) {

  /*
   * e.g.
   * driver_trip,prob
   * 1_1,1
   */
  override def toString = {
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
      } else if (featureVariant.equals("ext1")) {
        (DriveFeatures.pojoFields, DriveFeatures.nonFeatures, DriveFeatures.features)
      } else if (featureVariant.equals("ext2")) {
        (DriveFeatures2.pojoFields, DriveFeatures2.nonFeatures, DriveFeatures2.features)
      } else {
        throw new IllegalArgumentException("Feature set not supported!")
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

    val drivesDF = sqlCtx.createDataFrame(rowRDD, schema)

    //
    // Run Deep Learning for each driver
    //
    println("\n====> Running DeepLearning on the input data. \n")
    // Training data
    val train: H2OFrame = drivesDF

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
    dlParams._hidden = Array(12)
    // no dropping of constant colors
    dlParams._ignore_const_cols = false
    // number of passes over the training dataset to be carried out
    dlParams._epochs = 3
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

    drivesWErrRdd.saveAsTextFile(outputPath + "/wRawErrors/")

    /*
    // min und max per driver
    val driversMinMaxError = drivesWErrRdd.groupBy(_.driverId)
      .flatMap(group => {
      val driverId = group._1
      val minError = group._2.map(_.reconErr).min
      val maxError = group._2.map(_.reconErr).max

      Seq((driverId, (minError, maxError)))
    })


    val kaggleResultRdd = drivesWErrRdd.map(entry => (entry.driverId, entry))
      .leftOuterJoin(driversMinMaxError)
      .map(join => {
      val driveWReconErr = join._2._1
      val minErrorDriver = join._2._2.get._1
      val maxErrorDriver = join._2._2.get._2
      val prob = 1 - (driveWReconErr.reconErr - minErrorDriver) / (maxErrorDriver - minErrorDriver)

      new KaggleResult(driveWReconErr.driverId, driveWReconErr.driveId, prob)
    })*/

    /* global min und max */
    val globalMinError = drivesWErrRdd.map(_.reconErr).min()
    val globalMaxError = drivesWErrRdd.map(_.reconErr).max()

    val kaggleResultRdd = drivesWErrRdd.map(driver => {
      val prob = 1 - (driver.reconErr - globalMinError) / (globalMaxError - globalMinError)
      new KaggleResult(driver.driverId, driver.driveId, prob)
    })

    //
    // write the output file
    //
    println("\n====> Write the predictions to the output file. \n")
    kaggleResultRdd.saveAsTextFile(outputPath + "/kaggleResult")

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
    if (args.length == 5) {
      inputPath = args(0)
      featureVariant = args(1)
      outputPath = args(2)
      l1Norm = args(3).toDouble
      l2Norm = args(4).toDouble
      true
    } else {
      println(
        """
          |Failed executing OutlierDetection. Wrong number of arguments.
          |  Usage: FeatureExtractor <input path> <feature type> <output path> <L1> <L2>
        """.stripMargin)
      false
    }
  }

}

object OutlierDetection2 extends SparkContextSupport {

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
      } else if (featureVariant.equals("ext1")) {
        (DriveFeatures.pojoFields, DriveFeatures.nonFeatures, DriveFeatures.features)
      } else if (featureVariant.equals("ext2")) {
        (DriveFeatures2.pojoFields, DriveFeatures2.nonFeatures, DriveFeatures2.features)
      } else {
        throw new IllegalArgumentException("Feature set not supported!")
      }
    }

    //
    // Read CSV and transform into DataFrame
    //
    println("\n====> Running DeepLearning on the input data. \n")
    val dataRdd = sparkCtx.textFile(inputPath).cache()

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

    val drivesDF = sqlCtx.createDataFrame(rowRDD, schema).cache()
    drivesDF.registerTempTable("drives")

    var query = "SELECT DISTINCT driverId from drives"
    val driverIds = sqlCtx.sql(query).cache().vec(0).toDoubleArray

    var result: Option[RDD[DrivesWReconErrOpt]] = None
    var counter = 0

    for (driverId <- driverIds) {
      //
      // build a model for each driver
      //
      println("\n====> " + counter +  "/" + driverIds.length +
        " Building model for driver " + driverId +". \n")
      counter += 1

      // Training data
      query = "SELECT * FROM drives WHERE driverId = " + driverId
      val train: H2OFrame = sqlCtx.sql(query)

      /* Configure Deep Learning algorithm */
      val dlParams = new DeepLearningParameters()
      dlParams._train = train
      // ignore driverId, driveId
      dlParams._ignored_columns = nonFeatures
      // unsupervised: pick any non-constant column
      dlParams._response_column = Symbol(fieldNames(2))
      // the activation function (non-linearity) to be used the neurons in the hidden layers.
      dlParams._activation = DeepLearningParameters.Activation.MaxoutWithDropout
      // use the autoencoder, obviously
      dlParams._autoencoder = true
      // one hidden layer with 20 neurons
      dlParams._hidden = Array(10)
      // no dropping of constant colors
      dlParams._ignore_const_cols = false
      // number of passes over the training dataset to be carried out
      dlParams._epochs = 100
      // do some regularisation
      dlParams._l1 = this.l1Norm
      dlParams._l2 = this.l2Norm
      dlParams._loss = DeepLearningParameters.Loss.CrossEntropy


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
      val resultD = train.deepCopy(null)
      resultD.remove(features)
      resultD.add(reconstructionError)

      result match {
        case None => result = Some(h2oCtx.asRDD[DrivesWReconErrOpt](resultD).cache())
        case _ => result = Some(result.get.union(h2oCtx.asRDD[DrivesWReconErrOpt](resultD).cache()))
      }
    }

    val drivesWErrRdd: RDD[DrivesWReconErr] = h2oCtx.asRDD[DrivesWReconErrOpt](result.get)
      .map(entry =>
      DrivesWReconErr(entry.driverId.get.toInt, entry.driveId.get.toInt, entry.reconErr.get)
      )

    drivesWErrRdd.saveAsTextFile(outputPath + "/wRawErrors/")

    /*
    // min und max per driver
    val driversMinMaxError = drivesWErrRdd.groupBy(_.driverId)
      .flatMap(group => {
      val driverId = group._1
      val minError = group._2.map(_.reconErr).min
      val maxError = group._2.map(_.reconErr).max

      Seq((driverId, (minError, maxError)))
    })


    val kaggleResultRdd = drivesWErrRdd.map(entry => (entry.driverId, entry))
      .leftOuterJoin(driversMinMaxError)
      .map(join => {
      val driveWReconErr = join._2._1
      val minErrorDriver = join._2._2.get._1
      val maxErrorDriver = join._2._2.get._2
      val prob = 1 - (driveWReconErr.reconErr - minErrorDriver) / (maxErrorDriver - minErrorDriver)

      new KaggleResult(driveWReconErr.driverId, driveWReconErr.driveId, prob)
    })*/

    /* global min und max */
    val globalMinError = drivesWErrRdd.map(_.reconErr).min()
    val globalMaxError = drivesWErrRdd.map(_.reconErr).max()

    val kaggleResultRdd = drivesWErrRdd.map(driver => {
      val prob = 1 - (driver.reconErr - globalMinError) / (globalMaxError - globalMinError)
      new KaggleResult(driver.driverId, driver.driveId, prob)
    })

    //
    // write the output file
    //
    println("\n====> Write the predictions to the output file. \n")
    kaggleResultRdd.saveAsTextFile(outputPath + "/kaggleResult")

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
    if (args.length == 5) {
      inputPath = args(0)
      featureVariant = args(1)
      outputPath = args(2)
      l1Norm = args(3).toDouble
      l2Norm = args(4).toDouble
      true
    } else {
      println(
        """
          |Failed executing OutlierDetection. Wrong number of arguments.
          |  Usage: FeatureExtractor <input path> <feature type> <output path> <L1> <L2>
        """.stripMargin)
      false
    }
  }

}