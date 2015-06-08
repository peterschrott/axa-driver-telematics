package com.peedeex21.axa.model

import scala.collection.mutable.HashMap

/**
 * Created by peter on 04.06.15.
 */
class Drive() {

  object Drive {
    val deltaT = 1
  }

  var driverId = -1: Int
  var driveId = -1: Int
  var points = HashMap.empty[Int, Vector2D]

  /**
   * Constructor to initialize a drive.
   *
   * @param driverId identifier of the driver
   * @param driveId identifier of the drive of driver
   * @param content String containing lines of (x, y) coordinates of the drive
   */
  def this(driverId: Int, driveId: Int, content: String) {
    this()
    this.driverId = driverId
    this.driveId = driveId
    this.points = deserializeContent(content)
  }

  /**
   * Deserialize a String of lines of x, y coordinate tuples to a hash map.
   *
   * @param content String containing lines of (x, y) coordinates
   */
  def deserializeContent(content: String): HashMap[Int, Vector2D]  = {
    val tmpMap = HashMap.empty[Int, Vector2D]
    val contentArray = content.split("\n")
    for(i <- 1 until contentArray.size) {
      val lineArray = contentArray(i).split(",")
      tmpMap += (i-1 -> new Vector2D(lineArray(0).toDouble, lineArray(1).toDouble))
    }
    tmpMap
  }


  /**
   * container for the features
   */
  var distances = HashMap.empty[Int, Double]
  var distanceTotal = HashMap.empty[Int, Double]
  var speeds = HashMap.empty[Int, Double]
  var accelerations = HashMap.empty[Int, Double]
  var angles = HashMap.empty[Int, Double]

  /**
   * extracts all defined features
   */
  def extractAllFeatures() = {
    calculateDistances()
    calculateDistanceTotal()
    calculateSpeeds()
    calculateAccelerations()
    calculateAngels()
    this
  }

  /**
   * Fills the hash map [[distances]] by calculating the delta distance between consecutive
   * x, y coordinates in [[points]]
   */
  def calculateDistances(): Unit = {
    // no distance for first point
    distances += 0 -> 0
    // calculate the distances between consecutive points
    for(i <- 1 until points.size) {
      val pointPrev = points(i-1)
      val pointCurr = points(i)
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
  }

  /**
   * Fills the hash map [[speeds]] by using the [[distances]] and [[Drive.deltaT]]
   */
  def calculateSpeeds(): Unit = {
    if(distances.isEmpty) {
      throw new IllegalStateException("distance must be calculated first")
    }

    distances.map(distance => {
      speeds += (distance._1 -> distance._2 * Drive.deltaT)
    })
  }

  /**
   * Fills the hash map [[accelerations]] by using the [[distances]] and [[Drive.deltaT]]
   */
  def calculateAccelerations(): Unit = {
    if(distances.isEmpty) {
      throw new IllegalStateException("distance must be calculated first")
    }
    distances.map(distance => {
      accelerations += (distance._1 -> distance._2 * math.pow(Drive.deltaT, 2))
    })
  }

  /**
   * Fills the hash map [[angles]] by determining the angel between the vector to the previous and
   * future vertex
   */
  def calculateAngels(): Unit = {
    // no angle for the first and last vertex
    angles += (0 -> 0)
    angles += (points.size-1 -> 0)

    // calculate the angels
    for(i <- 1 until points.size-1) {
      val pointPrev = points(i-1)
      val pointCurr = points(i)
      val pointFuture = points(i+1)

      val v1 = pointCurr.subtract(pointPrev)
      val v2 = pointCurr.subtract(pointFuture)

      // alpha = arcCos( a dot b / |a| * |b| )
      val angle = math.acos(v1.dot(v2) / (v1.l2Norm() * v2.l2Norm()))
      angles += (i -> (if(angle isNaN) 0.0 else angle))
    }
  }

}
