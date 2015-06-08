package com.peedeex21.axa.model

/**
 * Created by Peer Schrott on 08.06.15.
 */
class Vector2D(val x: Double, val y: Double) {

  /**
   * Calculates the euclidean distance between this and other vector.
   *
   * @param other vector to calculate distance to
   * @return euclidean distance to other
   */
  def distanceTo(other: Vector2D) = {
    math.sqrt(math.pow(other.x - this.x, 2) + math.pow(other.y - this.y, 2))
  }

  /**
   * Add the other vector to this
   *
   * @param other vector to add to this
   * @return result of addition
   */
  def add(other: Vector2D) = {
    new Vector2D(this.x + other.x, this.y + other.y)
  }

  /**
   * Subtract the other vector from this
   *
   * @param other vector to subtract from this
   * @return result of subtraction
   */
  def subtract(other: Vector2D) = {
    new Vector2D(this.x - other.x, this.y - other.y)
  }

  /**
   * Dot product of this and other vector
   *
   * @param other vector to calculate dot product with
   * @return result of dot product as scalar
   */
  def dot(other: Vector2D) = {
    this.x * other.x + this.y * other.y
  }

  /**
   * Calculates the L2 norm (Euclidean Norm)
   *
   * @return L2 norm of this
   */
  def l2Norm() = {
    math.sqrt(math.pow(this.x, 2) + math.pow(this.y, 2))
  }

}