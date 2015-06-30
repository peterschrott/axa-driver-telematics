package com.peedeex21.axa.utils

/**
 * Created by peter on 30.06.15.
 */
object Utils {

  def median(s: List[Double]) = {
    val (lower, upper) = s.sortWith(_ < _).splitAt(s.size / 2)
    if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }

}
