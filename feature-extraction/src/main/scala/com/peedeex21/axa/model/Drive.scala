package com.peedeex21.axa.model

import scala.collection.mutable.HashMap

/**
 * Created by Peter Schrott on 04.06.15.
 */
@SerialVersionUID(1L)
class Drive() extends Serializable{

  var driverId = -1: Int
  var driveId = -1: Int
  var coordinates = HashMap.empty[Int, Vector2D]

  /**
   * Private constructor. Use companion object to initialize a drive.
   *
   * @param driverId identifier of the driver
   * @param driveId identifier of the drive of driver
   * @param content String containing lines of (x, y) coordinates of the drive
   */
  def this(driverId: Int, driveId: Int, content: String) {
    this()
    this.coordinates = deserializeContent(content)
    this.rowCount = coordinates.size
    this.driverId = driverId
    this.driveId = driveId
  }

  /**
   * Deserialize a String of lines of x, y coordinate tuples to a hash map.
   *
   * @param content String containing lines of (xs, y) coordinates
   */
  def deserializeContent(content: String): HashMap[Int, Vector2D]  = {
    val tmpMap = HashMap.empty[Int, Vector2D]
    val contentArray = content.split("\n")
    for(i <- 1 until contentArray.size) {
      val lineArray = contentArray(i).split(",")
      tmpMap += (i-1 -> Vector2D(lineArray(0).toDouble, lineArray(1).toDouble))
    }
    tmpMap
  }


  /**
   * container for level 1 features
   */
  var rowCount = 0: Int

  var distances = HashMap.empty[Int, Double]
  var distanceTotal = HashMap.empty[Int, Double]
  var speeds = HashMap.empty[Int, Double]
  var accelerations = HashMap.empty[Int, Double]
  var angles = HashMap.empty[Int, Double]

  var duration = 0.0
  var distance = 0.0

  var speedMin = Double.MaxValue
  var speedMean = 0.0
  var speedMax = Double.MinValue

  var accelerationMin = Double.MaxValue
  var accelerationMean = 0.0
  var accelerationMax = Double.MinValue

  var angleMean = 0.0

  var stopLengthCounter = 0
  var stopCounterTotal = 0
  var stops3Sec = 0
  var stops10Sec = 0
  var stops120Sec = 0

  /**
   * extracts level one features.
   * this are the features by each drive.
   */
  def extractLevelOneFeatures(): Unit = {
    calculateDuration()
    calculateDistances()
    calculateDistanceTotal()
    calculateSpeeds()
    calculateAccelerations()
    calculateAngels()
  }

  /**
   *
   */
  def calculateDuration(): Unit = {
    this.duration = rowCount * Drive.deltaT
  }

  /**
   * Fills the hash map [[distances]] by calculating the delta distance between consecutive
   * x, y coordinates in [[coordinates]]
   */
  def calculateDistances(): Unit = {
    // no distance for first point
    distances += 0 -> 0
    // calculate the distances between consecutive points
    for(i <- 1 until coordinates.size) {
      val pointPrev = coordinates(i-1)
      val pointCurr = coordinates(i)
      val d = pointPrev.distanceTo(pointCurr)
      distances += i -> d
    }
  }

  /**
   * Fills the hash map [[distanceTotal]] by using the [[distances]] and [[Drive.deltaT]]
   */
  def calculateDistanceTotal(): Unit = {
    if(distances.isEmpty) {
      throw new IllegalStateException("distance must be calculated first")
    }
    var sum = 0.0
    for(i <- 0 until distances.size){
      sum += distances(i)
      distanceTotal += (i -> sum)
    }

    distance = sum
  }

  /**
   * Fills the hash map [[speeds]] by using the [[distances]] and [[Drive.deltaT]]
   */
  def calculateSpeeds(): Unit = {
    if(distances.isEmpty) {
      throw new IllegalStateException("distance must be calculated first")
    }

    var speedSum = 0.0

    for(i <- 0 until distances.size){
      val distance = distances(i)
      val speed = distance / Drive.deltaT
      speeds += (i -> speed)

      // sum, min and max speed
      speedSum += speed
      if(speed < speedMin) speedMin = speed
      if(speed > speedMax) speedMax = speed

      // cont stops the driver made
      if(speed < 0.0001) {
        // update the stop counter if speed is 0
        stopLengthCounter += 1
        stopCounterTotal += 1
      } else {
        // if driving check if we just stopped and update the corresponding counters
        if(stopLengthCounter > (120*Drive.deltaT)) {
          stops120Sec += 1
        } else if(stopLengthCounter > (10*Drive.deltaT)) {
          stops10Sec += 1
        } else if(stopLengthCounter > (3*Drive.deltaT)) {
          stops3Sec += 1
        }
        // reset the stop counter
        stopLengthCounter = 0
      }
    }

    speedMean = speedSum / rowCount
  }

  /**
   * Fills the hash map [[accelerations]] by using the delta [[distances]] and [[Drive.deltaT]]
   */
  def calculateAccelerations(): Unit = {
    if(speeds.isEmpty) {
      throw new IllegalStateException("speeds must be calculated first")
    }

    var accelerationSum = 0.0

    accelerations += (0 -> 0)

    for(i <- 1 until speeds.size) {
      val speedCurr = speeds(i)
      val speedPrev = speeds(i-1)
      val acceleration = (speedCurr - speedPrev) / Drive.deltaT
      accelerations += (i -> acceleration)

      // sum, min and max
      accelerationSum += acceleration
      if(acceleration < accelerationMin) accelerationMin = acceleration
      if(acceleration > accelerationMax) accelerationMax = acceleration
    }

    accelerationMean = accelerationSum / rowCount
  }

  /**
   * Fills the hash map [[angles]] by determining the angel between the vector to the previous and
   * future vertex
   */
  def calculateAngels(): Unit = {
    // no angle for the first and last vertex
    angles += (0 -> 0)
    angles += (coordinates.size-1 -> 0)

    var angleSum = 0.0

    // calculate the angels
    for(i <- 1 until coordinates.size-1) {
      val pointPrev = coordinates(i-1)
      val pointCurr = coordinates(i)
      val pointFuture = coordinates(i+1)

      val v1 = pointCurr.subtract(pointPrev)
      val v2 = pointCurr.subtract(pointFuture)

      // alpha = arcCos( a dot b / |a| * |b| )
      var angle = math.acos(v1.dot(v2) / (v1.l2Norm() * v2.l2Norm()))
      angle = (if(angle.isNaN) 0.0 else angle)
      angles += (i -> angle)

      angleSum += angle
    }

    angleMean = angleSum / rowCount
  }

  /**
   *
   * @return
   */
  override def toString: String = {
    return driverId + "," + driveId  + "," + duration  + "," + distance  + "," +
      speedMin   + "," + speedMax   + "," + speedMean + "," +
      speedMeanDriver + "," + speedSDDriver  + "," +
      accelerationMin  + "," + accelerationMax   + "," +  accelerationMean + "," +
      accelerationMeanDriver + "," + accelerationSDDriver  + "," +
      angleMean  + "," + stops3Sec  + "," + stops10Sec  + "," + stops120Sec
  }

  /**
   * container for level 2 features.
   * this are features where all drives of one driver are compared.
   */
  var speedMeanDriver = 0.0
  var speedSDDriver = 0.0
  var accelerationMeanDriver = 0.0
  var accelerationSDDriver = 0.0

  def setAccelerationMeanDriver(accelerationMeanDriver: Double): Unit = {
    this.accelerationMeanDriver = accelerationMeanDriver
    this.accelerationSDDriver = this.accelerationMean - this.accelerationMeanDriver
  }

  def setSpeedMeanDriver(speedMeanDriver: Double): Unit = {
    this.speedMeanDriver = speedMeanDriver
    this.speedSDDriver = this.speedMean - this.speedMeanDriver
  }

  def transformToDriveMeta(): DriveMeta = {
    return new DriveMeta(driverId, driveId, duration, distance, speedMin, speedMax, speedMean,
      speedMeanDriver, speedSDDriver, accelerationMin, accelerationMax, accelerationMean,
      accelerationMeanDriver, accelerationSDDriver, angleMean, stopCounterTotal, stops3Sec,
      stops10Sec, stops120Sec)
  }

}

/**
 * Companion object for class [[Drive]].
 */
object Drive {

  /**
   * delta time for consecutive x, y coordinate tupel
   */
  val deltaT = 1

  def apply(driverId: Int, driveId: Int, content: String) = {
    new Drive(driverId, driveId, content)
  }

}
