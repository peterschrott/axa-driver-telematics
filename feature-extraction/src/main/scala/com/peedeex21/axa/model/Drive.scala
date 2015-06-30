package com.peedeex21.axa.model

import com.peedeex21.axa.utils.Utils

import scala.collection.mutable.HashMap

/**
 * Created by Peter Schrott on 04.06.15.
 */
@SerialVersionUID(1L)
class Drive() extends Serializable {

  val angleFeatureWindowSize = 10
  val topN = 0.05

  var driverId = -1: Int
  var driveId = -1: Int
  var coordinates = HashMap.empty[Int, Vector2D]

  /**
   * container for level 1 features
   */
  var rowCount = 0
  var topNValues = 0
  var distances = HashMap.empty[Int, Double]
  var distanceTotal = HashMap.empty[Int, Double]
  var speeds = HashMap.empty[Int, Double]
  var accelerations = HashMap.empty[Int, Double]
  var angles = HashMap.empty[Int, Double]
  var duration = 0.0
  var distance = 0.0
  var speedMin = Double.MaxValue
  var speedMean = 0.0
  var speedMedian = 0.0
  var speedMax = Double.MinValue
  var accelerationMin = Double.MaxValue
  var accelerationMean = 0.0
  var accelerationMedian = 0.0
  var accelerationMax = Double.MinValue
  var angleMean = 0.0
  var angleMedian = 0.0
  var stopLengthCounter = 0
  var stopCounterTotal = 0
  var stops3Sec = 0
  var stops10Sec = 0
  var stops120Sec = 0
  var turnsBiggerMean = 0
  var turns35 = 0
  var turns70 = 0
  var turns160 = 0
  var turnsU = 0

  /**
   * container for level 2 features.
   * this are features where all drives of one driver are compared.
   */
  var speedMeanDriver = 0.0
  var speedSDDriver = 0.0
  var accelerationMeanDriver = 0.0
  var accelerationSDDriver = 0.0

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
    this.topNValues = (rowCount * topN).toInt
    this.driverId = driverId
    this.driveId = driveId
  }

  /**
   * Deserialize a String of lines of x, y coordinate tuples to a hash map.
   *
   * @param content String containing lines of (xs, y) coordinates
   */
  def deserializeContent(content: String): HashMap[Int, Vector2D] = {
    val tmpMap = HashMap.empty[Int, Vector2D]
    val contentArray = content.split("\n")
    for (i <- 1 until contentArray.size) {
      val lineArray = contentArray(i).split(",")
      tmpMap += (i - 1 -> Vector2D(lineArray(0).toDouble, lineArray(1).toDouble))
    }
    tmpMap
  }

  /**
   * extracts level one features.
   * this are the features by each drive.
   */
  def extractLevelOneFeatures(): Unit = {
    calculateDuration()
    calculateDistanceFeatures()
    calculateSpeedFeatures()
    calculateAccelerations()
    calculateAngleFeatures()
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
   * And the hash map [[distanceTotal]] by using the [[distances]] and [[Drive.deltaT]]
   */
  def calculateDistanceFeatures(): Unit = {
    // no distance for first point
    this.distances += 0 -> 0

    var sum = 0.0

    // calculate the distances between consecutive points
    for (i <- 1 until this.coordinates.size) {
      val pointPrev = this.coordinates(i - 1)
      val pointCurr = this.coordinates(i)
      val d = pointPrev.distanceTo(pointCurr)
      this.distances += i -> d

      sum += d
      this.distanceTotal += (i -> sum)
    }

    this.distance = sum

  }

  /**
   * Fills the hash map [[speeds]] by using the [[distances]] and [[Drive.deltaT]]
   */
  def calculateSpeedFeatures(): Unit = {
    if (distances.isEmpty) {
      throw new IllegalStateException("distance must be calculated first")
    }

    // for top N percent
    var topMaxSpeeds = List[Double]()
    var min = Double.MaxValue
    var len = 0

    var speedSum = 0.0

    for (i <- 0 until distances.size) {
      val distance = distances(i)
      val speed = distance / Drive.deltaT
      speeds += (i -> speed)

      // sum, min and max speed
      speedSum += speed
      if (speed < speedMin) speedMin = speed

      if (len < topNValues || speed > min) {
        topMaxSpeeds = (speed :: topMaxSpeeds).sorted
        min = topMaxSpeeds.head
        len += 1
      }
      if (len > topNValues) {
        topMaxSpeeds = topMaxSpeeds.tail
        min = topMaxSpeeds.head
        len -= 1
      }

      // count number of stops the driver made
      if (speed < 0.0001) {
        // update the stop counter if speed is 0
        stopLengthCounter += 1
        stopCounterTotal += 1
      } else {
        // if driving check if we just stopped and update the corresponding counters
        if (stopLengthCounter > (120 * Drive.deltaT)) {
          stops120Sec += 1
        } else if (stopLengthCounter > (10 * Drive.deltaT)) {
          stops10Sec += 1
        } else if (stopLengthCounter > (3 * Drive.deltaT)) {
          stops3Sec += 1
        }
        // reset the stop counter
        stopLengthCounter = 0
      }
    }

    this.speedMax = topMaxSpeeds.foldLeft(0.0)(_ + _) / topNValues
    this.speedMean = speedSum / rowCount
    this.speedMedian = Utils.median(speeds.values.toList)
  }

  /**
   * Fills the hash map [[accelerations]] by using the delta [[distances]] and [[Drive.deltaT]]
   */
  def calculateAccelerations(): Unit = {
    if (speeds.isEmpty) {
      throw new IllegalStateException("speeds must be calculated first")
    }

    var accelerationSum = 0.0

    // top N percent
    var topMaxAccelerations = List[Double]()
    var min = Double.MaxValue
    var len = 0

    // no acceleration for first point
    this.accelerations += (0 -> 0)

    for (i <- 1 until speeds.size) {
      val speedCurr = speeds(i)
      val speedPrev = speeds(i - 1)
      val acceleration = (speedCurr - speedPrev) / Drive.deltaT
      this.accelerations += (i -> acceleration)

      // sum, min and max
      accelerationSum += acceleration
      if (acceleration < accelerationMin) accelerationMin = acceleration
      //if(acceleration > accelerationMax) accelerationMax = acceleration

      if (len < topNValues || acceleration > min) {
        topMaxAccelerations = (acceleration :: topMaxAccelerations).sorted
        min = topMaxAccelerations.head
        len += 1
      }
      if (len > topNValues) {
        topMaxAccelerations = topMaxAccelerations.tail
        min = topMaxAccelerations.head
        len -= 1
      }
    }

    this.accelerationMax = topMaxAccelerations.foldLeft(0.0)(_ + _) / topNValues
    this.accelerationMean = accelerationSum / rowCount
    this.accelerationMedian = Utils.median(accelerations.values.toList)
  }

  /**
   * Fills the hash map [[angles]] by determining the angle between the vector to the previous and
   * future vertex
   */
  def calculateAngleFeatures(): Unit = {
    // no angle for the first and last vertex
    this.angles += (0 -> 0)
    this.angles += (this.coordinates.size - 1 -> 0)

    var angleSum = 0.0
    var angleWindowSum = 0.0

    // calculate the angels
    for (i <- 1 until coordinates.size - 1) {
      val pointPrev = this.coordinates(i - 1)
      val pointCurr = this.coordinates(i)
      val pointFuture = this.coordinates(i + 1)

      // v1 = A - B = prev - curr
      // v2 = C - B = future - curr
      val v1 = pointPrev.subtract(pointCurr)
      val v2 = pointFuture.subtract(pointCurr)

      // calculate the turn (left or right)
      var turn = 1
      val cross = v1.y * v2.x - v1.x * v2.y
      if (cross > 0) turn = -1
      if (cross < 0) turn = 1

      // calculate the angle at current turn point
      // angle = arcCos( v1 dot v2 / |v1| * |v2| )
      var angle = math.acos(v1.dot(v2) / (v1.l2Norm() * v2.l2Norm()))
      angle = if (angle.isNaN) math.Pi else angle
      angle = math.Pi - angle
      angle = turn * angle

      this.angles += (i -> angle)

      angleSum += angle
      
      if(math.abs(angle) > math.Pi*0.90) {
        this.turnsU += 1
      }

      if (i < this.angleFeatureWindowSize) {
        angleWindowSum += angle
      } else {
        // for current window
        incrementTurns(angleWindowSum)
        // go to the next window
        angleWindowSum = angleWindowSum - this.angles(i - angleFeatureWindowSize) + angle
      }
    }

    // for the last window
    incrementTurns(angleWindowSum)

    // calculate the mean
    this.angleMean = angleSum / this.rowCount
    this.angleMedian = Utils.median(this.angles.values.toList)
  }

  /**
   * Increments counters for 4 different turn angles considering the sum of the last N angles.
   *
   * @param angleSum A sum of the last N angels.
   */
  def incrementTurns(angleSum: Double): Unit = {

    val angleSumTmp = math.abs(angleSum)

    if (angleSumTmp > math.toRadians(35)) {
      turns35 += 1
    } else if (angleSumTmp > math.toRadians(70)) {
      turns70 += 1
    } else if (angleSumTmp > math.toRadians(160)) {
      turns160 += 1
    } else if (angleSumTmp >= angleMean) {
      turnsBiggerMean += 1
    }
  }

  /**
   *
   * @return
   */
  override def toString: String = {
    return driverId + "," + driveId
  }

  def setAccelerationMeanDriver(accelerationMeanDriver: Double): Unit = {
    this.accelerationMeanDriver = accelerationMeanDriver
    this.accelerationSDDriver = this.accelerationMean - this.accelerationMeanDriver
  }

  def setSpeedMeanDriver(speedMeanDriver: Double): Unit = {
    this.speedMeanDriver = speedMeanDriver
    this.speedSDDriver = this.speedMean - this.speedMeanDriver
  }

  def transformToDriveMeta: DriveMeta = {
    new DriveMeta(driverId, driveId, duration, distance, speedMin, speedMax, speedMedian,
      speedMean, speedMeanDriver, speedSDDriver, accelerationMin, accelerationMax,
      accelerationMedian, accelerationMean, accelerationMeanDriver, accelerationSDDriver,
      angleMedian, angleMean, turns35, turns70, turns160, turnsBiggerMean, turnsU,
      stopCounterTotal, stops3Sec, stops10Sec, stops120Sec)
  }

}

/**
 * Companion object for class [[Drive]].
 */
object Drive {

  /**
   * delta time for consecutive x, y coordinate tuple
   */
  val deltaT = 1

  def apply(driverId: Int, driveId: Int, content: String): Drive = {
    new Drive(driverId, driveId, content)
  }

}
