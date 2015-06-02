package com.peedeex21.axa

import org.junit.Test


class PlottingUtilsTest {

  @Test
  def testPlotting(): Unit = {
    val input = List[(Int, Double)]((2002, 555.55), (2012, 1000.22))
    val name = "Average Budget per Year"

    /*val actChartPath = Paths.get(testutil.tempPath("util/plotting/avgBudgetPerYear.act.png"))
    val expChartPath = Paths.get(testutil.materializeResource("/util/plotting/avgBudgetPerYear.exp.png"))

    Files.deleteIfExists(actChartPath)
    assert(!Files.exists(actChartPath))

    plotting

    plotting.plotXYChart(input, actChartPath.toString, name)

    assume(Files.exists(actChartPath))
    assume(Files.exists(expChartPath))

    assert(java.util.Arrays.equals(Files.readAllBytes(actChartPath), Files.readAllBytes(expChartPath)))

    Files.delete(actChartPath)
    assert(!Files.exists(actChartPath))*/
  }
}