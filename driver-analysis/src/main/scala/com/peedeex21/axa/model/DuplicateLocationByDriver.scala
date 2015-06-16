package com.peedeex21.axa.model

/**
 * Created by Peter Schrott on 15.06.15.
 */
class DuplicateLocationByDriver {

  var driverId: Int = 0
  var x: Double = 0.0
  var y: Double= 0.0
  var count: Int = 0

  private def this(driverId: Int, x: Double, y: Double, count: Int) {
    this()
    this.driverId = driverId
    this.x = x
    this.y = y
    this.count = count
  }

  override def toString: String = {
    this.driverId + "," + this.x + "," + this.y + "," + this.count
  }

}

object DuplicateLocationByDriver {

  def apply(driverId: Int, x: Double, y: Double, count: Int): DuplicateLocationByDriver = {
    new DuplicateLocationByDriver(driverId, x, y, count)
  }

}
