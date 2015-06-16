package com.peedeex21.axa.algorithm

import com.peedeex21.axa.model.{DriveLog, DuplicateLocationByDriver}
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.common.{Parameter, ParameterMap}
import org.apache.flink.ml.pipeline.{TransformOperation, Transformer}

/**
 * Created by Peter Schrott on 15.06.15.
 */
class DuplicateLocations extends Transformer[DuplicateLocations] {

  import DuplicateLocations._

  /** Sets the minimum count for points which are returned
    *
    * @param minimumCount
    * @return itself
    */
  def setMinimumCount(minimumCount: Int): DuplicateLocations = {
    parameters.add(MinimumCount, minimumCount)
    this
  }

}

object DuplicateLocations {

  case object MinimumCount extends Parameter[Int] {
    val defaultValue: Option[Int] = Some(1)
  }

  def apply(): DuplicateLocations = {
    new DuplicateLocations()
  }

  implicit def transformDuplicateLocations = {
    new TransformOperation[DuplicateLocations, DriveLog, DuplicateLocationByDriver] {
      override def transform(instance: DuplicateLocations, transformParameters: ParameterMap,
                             input: DataSet[DriveLog]): DataSet[DuplicateLocationByDriver] = {

        val resultingParameters = instance.parameters ++ transformParameters

        val minCount = resultingParameters(MinimumCount)

        input.map(log => DuplicateLocationByDriver(log.driverId, log.x, log.y, 1))
          .groupBy(log => {
            (log.driverId, log.x, log.y)
          })
          .reduce((d1, d2) => {
            DuplicateLocationByDriver(d1.driverId, d1.x, d1.y, (d1.count + d2.count))
          }).filter(_.count > minCount)
      }
    }
  }

}
