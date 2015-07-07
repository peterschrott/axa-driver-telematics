package com.peedeex21.axa.model

/**
  * Created by Peter Schrott
  */
@SerialVersionUID(1L)
class DriveLog extends Serializable {

   var driverId: Int = 0
   var driveId: Int = 0
   var seqNo: Int = 0
   var x: Double = 0.0
   var y: Double = 0.0
   var distanceDelta: Double = 0.0
   var distanceTotal: Double = 0.0
   var speedDelta: Double = 0.0
   var accelerationDelta: Double = 0.0
   var angleDelta: Double = 0.0

   def this(driverId: Int, driveId: Int, seqNo: Int, x: Double, y: Double) = {
     this()
     this.driverId = driverId
     this.driveId = driveId
     this.seqNo = seqNo
     this.x = x
     this.y = y
   }

   def this(driverId: Int, driveId: Int, seqNo: Int, x: Double, y: Double, distanceDelta: Double,
            distanceTotal: Double, speedDelta: Double, accelerationDelta: Double,
            angleDelta: Double) = {
     this()
     this.driverId = driverId
     this.driveId = driveId
     this.seqNo = seqNo
     this.x = x
     this.y = y
     this.distanceDelta = distanceDelta
     this.distanceTotal = distanceTotal
     this.speedDelta = speedDelta
     this.accelerationDelta = accelerationDelta
     this.angleDelta = angleDelta
   }

   override def toString: String = {
     driverId + "," + driveId + "," + seqNo + "," + x + "," + y + "," + distanceDelta + "," +
       distanceTotal + "," + speedDelta + "," + accelerationDelta + "," + angleDelta
   }
 }