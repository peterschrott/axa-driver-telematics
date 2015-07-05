package com.peedeex21.axa

import java.io.File

import com.peedeex21.axa.model.DriveMeta
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkFiles}
import water.app.SparkContextSupport

object OutlierDetection extends SparkContextSupport {

  private var inputPath: String = null
  private var targetFiles: String = null

  def main(args: Array[String]): Unit = {
    
    if(!parseParameters(args)) {
      return
    }
    
    // Create Spark context which will drive computation.
    val conf = configure("Sparkling Water: Deep Learning on AXA Driver Telematics")
    val sc = new SparkContext(conf)
    sc.addFile(absPath(this.inputPath), true)

    // Run H2O cluster inside Spark cluster
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    //
    // Load H2O from CSV file (i.e., access directly H2O cloud)
    // Use super-fast advanced H2O CSV parser !!!
    val driveData = new H2OFrame(new File(SparkFiles.get(this.targetFiles)))

    //
    // Use H2O to RDD transformation
    //
    val airlinesTable: RDD[DriveMeta] = asRDD[DriveMeta](driveData)
    println(s"\n===> Number of all drives via RDD#count call: ${airlinesTable.count()}\n")
    println(s"\n===> Number of all drives via H2O#Frame#count: ${driveData.numRows()}\n")

    //
    // Filter data with help of Spark SQL
    //
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._ // import implicit conversions
    airlinesTable.toDF.registerTempTable("driveTable")

    // Select only interesting columns and flights with destination in SFO
    val query = "SELECT * FROM driveTable WHERE driverId = 1"
    val result: H2OFrame = sqlContext.sql(query) // Using a registered context and tables
    println(s"\n===> Number of drives of driverId = 1: ${result.numRows()}\n")

    //
    // Run Deep Learning
    //
/*
    println("\n====> Running DeepLearning on the result of SQL query\n")
    // Training data
    val train = result('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime,
      'UniqueCarrier, 'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest,
      'Distance, 'IsDepDelayed )
    train.replace(train.numCols()-1, train.lastVec().toEnum)
    train.update(null)

    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._response_column = 'IsDepDelayed

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    //
    // Use model for scoring
    //
    println("\n====> Making prediction with help of DeepLearning model\n")
    val predictionH2OFrame = dlModel.score(result)('predict)
    val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map ( _.result.getOrElse("NaN") )
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))*/

    // Stop Spark cluster and destroy all executors
    if (System.getProperty("spark.ext.h2o.preserve.executors")==null) {
      sc.stop()
    }
    // This will block in cluster mode since we have H2O launched in driver
  }

  /**
   *
   * @param args start parameter
   * @return true if parsing of arguments was successful, otherwise false
   */
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 1) {
      inputPath = args(0)
      targetFiles = "1" //inputPath.substring(inputPath.lastIndexOf("/") + 1)
      true
    } else {
      System.out.println("Failed executing OutlierDetection.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: FeatureExtractor <input path>")
      false
    }
  }

}