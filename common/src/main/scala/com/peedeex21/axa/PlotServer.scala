package com.peedeex21.axa

import com.quantifind.charts.Highcharts._

/**
 * Created by Peter Schrott on 02.06.15.
 */
object PlotServer {

  var port = 54321

  def main (args: Array[String]) {

    if (!parseParameters(args)) {
      return
    }

    setPort(port)
  }

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 1) {
      port = args(0).toInt
      true
    } else {
      System.out.println("Executing AXA PlotServer on default port 54321.")
      System.out.println("  Usage: PlotServer <port>")
      true
    }
  }

}
