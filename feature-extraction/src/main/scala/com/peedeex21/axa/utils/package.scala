package com.peedeex21.axa

/**
 * Created by peter on 30.06.15.
 */
package object extentions {

  implicit def iterExt[A](iter: Iterable[A]) = new {
    def top[B](n: Int, f: A => B)(implicit ord: Ordering[B]): List[A] = {
      def updateSofar(sofar: List[A], el: A): List[A] = {
        if (ord.compare(f(el), f(sofar.head)) > 0)
          (el :: sofar.tail).sortBy(f)
        else sofar
      }

      val (sofar, rest) = iter.splitAt(n)
      (sofar.toList.sortBy(f) /: rest)(updateSofar(_, _)).reverse
    }
  }
}
