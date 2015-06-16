package com.peedeex21.axa.model

/**
 * Created by Peter Schrott on 15.06.15.
 */
class DriveLog() {

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

  private def this(driverId: Int, driveId: Int, seqNo: Int, x: Double, y: Double) = {
    this()
    this.driverId = driverId
    this.driveId = driveId
    this.seqNo = seqNo
    this.x = x
    this.y = y
  }

  override def toString: String = {
    driverId + "," + driveId + "," + x + "," + y  + "," + distance + "," + distanceTotal + "," +
      speed  + ","  +acceleration  + "," + angle
  }
}

object DriveLog {

  def apply(driverId: Int, driveId: Int, seqNo: Int, x: Double, y: Double): DriveLog = {
    new DriveLog(driverId, driveId, seqNo, x, y)
  }

}
