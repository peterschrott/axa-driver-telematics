package com.peedeex21.axa.model

import org.apache.flink.ml.math.DenseVector

/**
 * Created by Peter Schrott
 */
@SerialVersionUID(1L)
class DriveMeta extends Serializable{

  var driverId = 0
  var driveId = 0

  var duration = 0.0
  var distance = 0.0

  var speedMin = 0.0
  var speedMean = 0.0
  var speedMax = 0.0
  var speedMeanDriver = 0.0
  var speedSDDriver = 0.0

  var accelerationMin = 0.0
  var accelerationMean = 0.0
  var accelerationMax = 0.0
  var accelerationMeanDriver = 0.0
  var accelerationSDDriver = 0.0

  var angleMean = 0.0

  var stopCounterTotal = 0
  var stops3Sec = 0
  var stops10Sec = 0
  var stops120Sec = 0

  def this(driverId: Int, driveId: Int, duration: Double, distance: Double, speedMin: Double,
           speedMax: Double, speedMean: Double, speedMeanDriver: Double, speedSDDriver: Double,
           accelerationMin: Double, accelerationMax: Double, accelerationMean: Double,
           accelerationMeanDriver: Double, accelerationSDDriver: Double, angleMean: Double,
           stopCounterTotal: Int, stops3Sec: Int, stops10Sec: Int, stops120Sec: Int) {
    this()
    this.driverId = driverId
    this.driveId = driveId
    this.duration = duration
    this.distance = distance
    this.speedMin = speedMin
    this.speedMax = speedMax
    this.speedMean = speedMean
    this.speedMeanDriver = speedMeanDriver
    this.speedSDDriver = speedSDDriver
    this.accelerationMin = accelerationMin
    this.accelerationMax = accelerationMax
    this.accelerationMean = accelerationMean
    this.accelerationMeanDriver = accelerationMeanDriver
    this.accelerationSDDriver = accelerationSDDriver
    this.angleMean = angleMean
    this.stopCounterTotal = stopCounterTotal
    this.stops3Sec = stops3Sec
    this.stops10Sec = stops10Sec
    this.stops120Sec = stops120Sec
  }

  def getFeatureVector(): DenseVector = {
    val features =  Array(duration, distance, speedMin, speedMax, speedMean, speedMeanDriver,
      speedSDDriver, accelerationMin, accelerationMax, accelerationMean, accelerationMeanDriver,
      accelerationSDDriver, angleMean, stopCounterTotal, stops3Sec, stops10Sec, stops120Sec)
    DenseVector(features)
  }

  override def toString: String = {
    return driverId + "," + driveId  + "," + duration  + "," + distance  + "," +
      speedMin   + "," + speedMax   + "," + speedMean + "," +
      speedMeanDriver + "," + speedSDDriver  + "," +
      accelerationMin  + "," + accelerationMax   + "," +  accelerationMean + "," +
      accelerationMeanDriver + "," + accelerationSDDriver  + "," +
      angleMean  + "," + stopCounterTotal   + "," + stops3Sec  + "," + stops10Sec  + "," + stops120Sec
  }

}
