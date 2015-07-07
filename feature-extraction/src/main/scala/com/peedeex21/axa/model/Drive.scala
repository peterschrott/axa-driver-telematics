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

  var distanceDeltaMap = HashMap.empty[Int, Double]
  var distanceTotalMap = HashMap.empty[Int, Double]
  var speedDeltaMap = HashMap.empty[Int, Double]
  var accDeltaMap = HashMap.empty[Int, Double]
  var angleDeltaMap = HashMap.empty[Int, Double]

  var duration = 0.0
  var distance = 0.0
  var speedMin = Double.MaxValue
  var speedMean = 0.0
  var speedMeanDeviation = 0.0
  var speedSd = 0.0
  var speedMedian = 0.0
  var speedMax = Double.MinValue
  var accMin = Double.MaxValue
  var accMean = 0.0
  var accMeanDeviation = 0.0
  var accSd = 0.0
  var accMedian = 0.0
  var accMax = Double.MinValue
  var angleMean = 0.0
  var angleMedian = 0.0
  var stopLengthCounter = 0.0
  var stopCounterTotal = 0.0
  var stops1Sec = 0.0
  var stops3Sec = 0.0
  var stops10Sec = 0.0
  var stops120Sec = 0.0
  var stopDriveRatio = 0.0
  var turnsTotal = 0.0
  var turnsBiggerMean = 0.0
  var turns35P = 0.0
  var turns35N = 0.0
  var turns70P = 0.0
  var turns70N = 0.0
  var turns160P = 0.0
  var turns160N = 0.0
  var turnsU = 0.0

  /**
   * container for level 2 features.
   * this are features where all drives of one driver are compared.
   */
  var speedMeanDriver = 0.0
  var speedSdDriver = 0.0
  var accMeanDriver = 0.0
  var accSdDriver = 0.0

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
   * @param content String containing lines of (x, y) coordinates
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
   * Fills the hash map [[distanceDeltaMap]] by calculating the delta distance between consecutive
   * x, y coordinates in [[coordinates]]
   * And the hash map [[distanceTotalMap]] by using the [[distanceDeltaMap]] and [[Drive.deltaT]]
   */
  def calculateDistanceFeatures(): Unit = {
    // no distance for first point
    this.distanceDeltaMap += (0 -> 0)
    this.distanceTotalMap += (0 -> 0)

    var sum = 0.0

    // calculate the distances between consecutive points
    for (i <- 1 until this.coordinates.size) {
      val pointPrev = this.coordinates(i - 1)
      val pointCurr = this.coordinates(i)
      val d = pointPrev.distanceTo(pointCurr)
      this.distanceDeltaMap += (i -> d)

      sum += d
      this.distanceTotalMap += (i -> sum)
    }

    this.distance = sum

    val distanceMean = this.distance / rowCount

    for (i <- 0 until this.distanceDeltaMap.size) {
      val deviation = this.distanceDeltaMap(i) - distanceMean
      if (math.abs(deviation) > (4 * distanceMean)) {
        this.distanceDeltaMap(i) = distanceMean
      }
    }
  }

  /**
   * Fills the hash map [[speedDeltaMap]] by using the [[distanceDeltaMap]] and [[Drive.deltaT]]
   */
  def calculateSpeedFeatures(): Unit = {
    if (distanceDeltaMap.isEmpty) {
      throw new IllegalStateException("distance must be calculated first")
    }

    // for top N percent
    var topMaxSpeeds = List[Double]()
    var min = Double.MaxValue
    var len = 0

    var speedSum = 0.0

    for (i <- 0 until distanceDeltaMap.size) {
      val distance = distanceDeltaMap(i)
      val speed = distance / Drive.deltaT
      speedDeltaMap += (i -> speed)

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
        } else if (stopLengthCounter > (1 * Drive.deltaT)) {
          stops1Sec += 1
        }
        // reset the stop counter
        stopLengthCounter = 0
      }
    }

    this.stops120Sec = (this.stops120Sec * 120) / this.stopCounterTotal
    this.stops10Sec = (this.stops10Sec * 10) / this.stopCounterTotal
    this.stops3Sec = (this.stops3Sec * 3) / this.stopCounterTotal
    this.stops1Sec = (this.stops1Sec * 1) / this.stopCounterTotal
    this.stopDriveRatio = this.stopCounterTotal / this.duration

    this.speedMax = topMaxSpeeds.foldLeft(0.0)(_ + _) / topNValues
    this.speedMean = speedSum / rowCount
    this.speedMedian = Utils.median(speedDeltaMap.values.toList)

    var sumVariance = 0.0
    for (i <- 1 until speedDeltaMap.size) {
      sumVariance += math.pow((speedDeltaMap(i) - speedMean), 2)
    }
    this.speedSd = math.sqrt(sumVariance/this.rowCount)

  }

  /**
   * Fills the hash map [[accDeltaMap]] by using the delta [[distanceDeltaMap]] and [[Drive.deltaT]]
   */
  def calculateAccelerations(): Unit = {
    if (this.speedDeltaMap.isEmpty) {
      throw new IllegalStateException("speeds must be calculated first")
    }

    var accelerationSum = 0.0

    // top N percent
    var topMaxAccelerations = List[Double]()
    var min = Double.MaxValue
    var len = 0

    // no acceleration for first point
    this.accDeltaMap += (0 -> 0)

    for (i <- 1 until this.speedDeltaMap.size) {
      val speedCurr = this.speedDeltaMap(i)
      val speedPrev = this.speedDeltaMap(i - 1)
      val acceleration = (speedCurr - speedPrev) / Drive.deltaT
      this.accDeltaMap += (i -> acceleration)

      // sum, min and max
      accelerationSum += acceleration
      if (acceleration < this.accMin) this.accMin = acceleration

      if (len < this.topNValues || acceleration > min) {
        topMaxAccelerations = (acceleration :: topMaxAccelerations).sorted
        min = topMaxAccelerations.head
        len += 1
      }
      if (len > this.topNValues) {
        topMaxAccelerations = topMaxAccelerations.tail
        min = topMaxAccelerations.head
        len -= 1
      }
    }

    this.accMax = topMaxAccelerations.foldLeft(0.0)(_ + _) / this.topNValues
    this.accMean = accelerationSum / this.rowCount
    this.accMedian = Utils.median(this.accDeltaMap.values.toList)

    var sumVariance = 0.0
    for (i <- 1 until this.accDeltaMap.size) {
      sumVariance += math.pow((this.accDeltaMap(i) - this.accDeltaMap(i)), 2)
    }
    this.accSd = math.sqrt(sumVariance/this.rowCount)
  }

  /**
   * Fills the hash map [[angleDeltaMap]] by determining the angle between the vector to the previous and
   * future vertex
   */
  def calculateAngleFeatures(): Unit = {
    // no angle for the first and last vertex
    this.angleDeltaMap += (0 -> 0)
    this.angleDeltaMap += (this.coordinates.size - 1 -> 0)

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

      this.angleDeltaMap += (i -> angle)

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
        angleWindowSum = angleWindowSum - this.angleDeltaMap(i - this.angleFeatureWindowSize) + angle
      }
    }

    // for the last window
    incrementTurns(angleWindowSum)

    this.turnsBiggerMean /= turnsTotal
    this.turns35N /= turnsTotal
    this.turns35P /= turnsTotal
    this.turns70N /= turnsTotal
    this.turns70P /= turnsTotal
    this.turns160N /= turnsTotal
    this.turns160P /= turnsTotal
    this.turnsU /= turnsTotal

    // calculate the mean
    this.angleMean = angleSum / this.rowCount
    this.angleMedian = Utils.median(this.angleDeltaMap.values.toList)
  }

  /**
   * Increments counters for 4 different turn angles considering the sum of the last N angles.
   *
   * @param angleSum A sum of the last N angels.
   */
  def incrementTurns(angleSum: Double): Unit = {

    val angleSumTmp = math.abs(angleSum)

    /* count all turns bigger than 35 deg to normalize the others */
    if (angleSumTmp > math.toRadians(35)) {
      this.turnsTotal += 1
    }

    if (angleSumTmp >= angleMean) {
      this.turnsBiggerMean += 1
    }

    if (angleSumTmp > math.toRadians(160)) {
      if(math.signum(angleSum) > 0) {
        this.turns160P += 1
      } else {
        this.turns160N += 1
      }

    } else if (angleSumTmp > math.toRadians(70)) {
      if(math.signum(angleSum) > 0) {
        this.turns70P += 1
      } else {
        this.turns70N += 1
      }
    } else if (angleSumTmp > math.toRadians(35)) {
      if(math.signum(angleSum) > 0) {
        this.turns35P += 1
      } else {
        this.turns35N += 1
      }
    }
  }

  /**
   *
   * @return
   */
  override def toString: String = {
    driverId + "," + driveId + "," + speedMean + "," + speedMeanDeviation + "," + speedMeanDriver
  }

  def setAccMeanDriver(accMeanDriver: Double): Unit = {
    this.accMeanDriver = accMeanDriver
    this.accMeanDeviation = this.accMean - this.accMeanDriver
  }

  def setSpeedMeanDriver(speedMeanDriver: Double): Unit = {
    this.speedMeanDriver = speedMeanDriver
    this.speedMeanDeviation = this.speedMean - this.speedMeanDriver
  }

  def setAccSdDriver(accSDDriver: Double): Unit = {
    this.accSdDriver = accSDDriver
  }

  def setSpeedSdDriver(speedSDDriver: Double): Unit = {
    this.speedSdDriver = speedSDDriver
  }

  def transformToDriveMeta: DriveMeta = {
    new DriveMeta(this.driverId, this.driveId, this.duration, this.distance, this.speedMin,
      this.speedMax, this.speedMedian, this.speedMean, this.speedMeanDeviation, this.speedSd,
      this.speedMeanDriver, this.speedSdDriver, this.accMin, this.accMax, this.accMedian,
      this.accMean, this.accMeanDeviation, this.accSd, this.accMeanDriver, this.accSdDriver,
      this.angleMedian, this.angleMean, this.turns35P, this.turns35N, this.turns70P,
      this.turns70N, this.turns160P, this.turns160N, this.turnsBiggerMean, this.turnsU,
      this.stopDriveRatio, this.stops1Sec, this.stops3Sec, this.stops10Sec, this.stops120Sec)
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
