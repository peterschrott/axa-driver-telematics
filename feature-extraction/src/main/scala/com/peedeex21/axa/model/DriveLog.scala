package com.peedeex21.axa.model

/**
 * Created by Peter Schrott
 */
@SerialVersionUID(1L)
class DriveLog extends Serializable{

  var driverId: Int = 0
  var driveId: Int = 0
  var seqNo: Int = 0
  var x: Double = 0.0
  var y: Double = 0.0
  var distance: Double = 0.0
  var distanceTotal: Double = 0.0
  var speed: Double = 0.0
  var acceleration: Double = 0.0
  var angle: Double = 0.0

  def this(driverId: Int, driveId: Int, seqNo: Int, x: Double, y: Double) = {
    this()
    this.driverId = driverId
    this.driveId = driveId
    this.seqNo = seqNo
    this.x = x
    this.y = y
  }

  def this(driverId: Int, driveId: Int, seqNo: Int, x: Double, y: Double, distance: Double,
                    distanceTotal: Double, speed: Double, acceleration: Double, angle: Double) = {
    this()
    this.driverId = driverId
    this.driveId = driveId
    this.seqNo = seqNo
    this.x = x
    this.y = y
    this.distance = distance
    this.distanceTotal = distanceTotal
    this.speed = speed
    this.acceleration = acceleration
    this.angle = angle
  }

  override def toString: String = {
    driverId + "," + driveId + "," + x + "," + y  + "," + distance + "," + distanceTotal + "," +
      speed  + ","  +acceleration  + "," + angle
  }
}