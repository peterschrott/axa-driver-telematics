package com.peedeex21

import com.quantifind.charts.Highcharts._

/**
 * Created by peter on 29.05.15.
 */
package object plotting {

  def plotXYScatter(in: Seq[(Double, Double)]): Unit = {
    scatter(in)
  }

}
