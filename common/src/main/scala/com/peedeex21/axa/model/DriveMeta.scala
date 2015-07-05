package com.peedeex21.axa.model

import org.apache.flink.ml.math.DenseVector

/**
 * Created by Peter Schrott
 */
@SerialVersionUID(1L)
class DriveMeta extends Product with Serializable {

  var driverId = 0.0
  var driveId = 0.0

  var duration = 0.0
  var distance = 0.0

  var speedMin = 0.0
  var speedMax = 0.0
  var speedMedian = 0.0
  var speedMean = 0.0
  var speedMeanDeviation = 0.0
  var speedSd = 0.0
  var speedMeanDriver = 0.0
  var speedSdDriver = 0.0

  var accMin = 0.0
  var accMax = 0.0
  var accMedian = 0.0
  var accMean = 0.0
  var accMeanDeviation = 0.0
  var accSd = 0.0
  var accMeanDriver = 0.0
  var accSdDriver = 0.0

  var angleMedian = 0.0
  var angleMean = 0.0
  var turns35P = 0.0
  var turns35N = 0.0
  var turns70P = 0.0
  var turns70N = 0.0
  var turns160P = 0.0
  var turns160N = 0.0
  var turnsBiggerMean = 0.0
  var turnsU = 0.0

  var stopDriveRatio = 0.0
  var stops1Sec = 0.0
  var stops3Sec = 0.0
  var stops10Sec = 0.0
  var stops120Sec = 0.0

  def this(driverId: Int, driveId: Int, duration: Double, distance: Double, speedMin: Double,
           speedMax: Double, speedMedian: Double, speedMean: Double, speedMeanDeviation: Double,
           speedSd: Double, speedMeanDriver: Double, speedSdDriver: Double, accMin: Double,
           accMax: Double, accMedian: Double, accMean: Double, accMeanDeviation: Double,
           accSd: Double, accMeanDriver: Double, accSdDriver: Double, angleMedian: Double,
           angleMean: Double, turns35P: Double,  turns35N: Double, turns70P: Double,
           turns70N: Double, turns160P: Double, turns160N: Double, turnsBiggerMean: Double,
           turnsU: Double, stopDriveRatio: Double, stops1Sec: Double, stops3Sec: Double,
           stops10Sec: Double, stops120Sec: Double) {
    this()
    this.driverId = driverId
    this.driveId = driveId
    this.duration = duration
    this.distance = distance
    this.speedMin = speedMin
    this.speedMax = speedMax
    this.speedMedian = speedMedian
    this.speedMean = speedMean
    this.speedMeanDeviation = speedMeanDeviation
    this.speedSd = speedSd
    this.speedMeanDriver = speedMeanDriver
    this.speedSdDriver = speedSdDriver
    this.accMin = accMin
    this.accMax = accMax
    this.accMedian = accMedian
    this.accMean = accMean
    this.accMeanDeviation = accMeanDeviation
    this.accSd = accSd
    this.accMeanDriver = accMeanDriver
    this.accSdDriver = accSdDriver
    this.angleMedian = angleMedian
    this.angleMean = angleMean
    this.turns35P = turns35P
    this.turns35N = turns35N
    this.turns70P = turns70P
    this.turns70N = turns70N
    this.turns160P = turns160P
    this.turns160N = turns160N
    this.turnsBiggerMean = turnsBiggerMean
    this.turnsU = turnsU
    this.stopDriveRatio = stopDriveRatio
    this.stops1Sec = stops1Sec
    this.stops3Sec = stops3Sec
    this.stops10Sec = stops10Sec
    this.stops120Sec = stops120Sec
  }

  def toFeatureVector: DenseVector = {
    DenseVector(Array(duration, distance, speedMin, speedMax, speedMean, speedMeanDriver,
      speedSdDriver, accMin, accMax, accMean, accMeanDriver,
      accSdDriver, angleMean, stopDriveRatio, stops3Sec, stops10Sec, stops120Sec))
  }

  override def toString: String = {
    driverId + "," + driveId + "," + duration + "," + distance + "," +
      speedMax + "," + speedMedian + "," + speedMean + "," + speedMeanDeviation + "," + speedSd + "," +
      speedMeanDriver + "," + speedSdDriver + "," +
      accMax + "," + accMedian + "," + accMean + "," + accMeanDeviation + "," + accSd + "," +
      accMeanDriver + "," + accSdDriver + "," +
      angleMedian + "," + angleMean + "," +
      turns35P + "," + turns35N + "," + turns70P + "," + turns70N + "," +
      turns160P + "," + turns160N + "," + turnsBiggerMean + "," + turnsU + "," +
      stopDriveRatio + "," + stops1Sec + "," + stops3Sec + "," + stops10Sec + "," + stops120Sec
  }

  override def productElement(n: Int) = n match {
    case 0 => driverId
    case 1 => driveId
    case 2 => duration
    case 3 => distance
    case 4 => speedMax
    case 5 => speedMedian
    case 6 => speedMean
    case 7 => speedMeanDeviation
    case 8 => speedSd
    case 9 => speedMeanDriver
    case 10 => speedSdDriver
    case 11 => accMax
    case 12 => accMedian
    case 13 => accMean
    case 14 => accMeanDeviation
    case 15 => accSd
    case 16 => accMeanDriver
    case 17 => accSdDriver
    case 18 => angleMedian
    case 19 => angleMean
    case 20 => turns35P
    case 21 => turns35N
    case 22 => turns70P
    case 23 => turns70N
    case 24 => turns160P
    case 25 => turns160N
    case 26 => turnsBiggerMean
    case 27 => turnsU
    case 28 => stopDriveRatio
    case 29 => stops1Sec
    case 30 => stops3Sec
    case 31 => stops10Sec
    case 32 => stops120Sec
    case  _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productArity: Int = 33

  override def canEqual(that: Any): Boolean = that.isInstanceOf[DriveMeta]

}
