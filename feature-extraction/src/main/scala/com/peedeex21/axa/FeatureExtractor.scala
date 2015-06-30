package com.peedeex21.axa

import com.peedeex21.axa.model.{Drive, DriveLog, DriveMeta}
import org.apache.flink.api.scala._

/**
 * Created by Peter Schrott
 */
class FeatureExtractor(env: ExecutionEnvironment) {

  /**
   * extract some nice features for each drive :)
   *
   * level 1 features: features, describing a single drive only
   * level 2 features: features, describing drives in correlation of their sibling drives
   */
  def extract(driveDS: DataSet[Drive]): (DataSet[DriveMeta], DataSet[DriveLog]) = {
    /* level 1 features */
    val featureL1DS = driveDS.map(drive => {
      drive.extractLevelOneFeatures()
      drive
    })

    /* level 2 features */
    val meansByDriverDrive =
      featureL1DS.map(entry => {
        (entry.driverId, entry.driveId, entry.rowCount, entry.speedMean, entry.accelerationMean)
      })
        .groupBy(agg => (agg._1, agg._2))
        .reduce((agg1, agg2) => (agg1._1, agg1._2, agg1._3, (agg1._4 + agg2._4), (agg1._5 + agg2._5)))
        .map(agg => (agg._1, agg._2, agg._4 / agg._3, agg._5 / agg._3))

    val featureL2DS = featureL1DS.join(meansByDriverDrive)
      .where(d => (d.driverId, d.driveId)).equalTo(agg => (agg._1, agg._2))
      .map(product => {
      product._1.setSpeedMeanDriver(product._2._3)
      product._1.setAccelerationMeanDriver(product._2._4)
      product._1
    })

    val driveMeta = featureL2DS.map(entry => entry.transformToDriveMeta)

    // join the features with the original data set of x and
    // as join key the driver id, drive id and sequence number is used
    var driveLogDS = featureL1DS
      .flatMap(entry => {
      entry.coordinates.map(a => {
        new DriveLog(entry.driveId, entry.driverId, a._1, a._2.x, a._2.y)
      })
    })

    val distanceDS = featureL1DS
      .flatMap(entry => {
      entry.distances.map(a => {
        (entry.driveId, entry.driverId, a._1, a._2)
      })
    })

    val distanceTotalDS = featureL1DS
      .flatMap(entry => {
      entry.distanceTotal.map(a => {
        (entry.driveId, entry.driverId, a._1, a._2)
      })
    })

    val speedDS = featureL1DS
      .flatMap(entry => {
      entry.speeds.map(a => {
        (entry.driveId, entry.driverId, a._1, a._2)
      })
    })

    val accelerationDS = featureL1DS
      .flatMap(entry => {
      entry.accelerations.map(a => {
        (entry.driveId, entry.driverId, a._1, a._2)
      })
    })

    val angleDS = featureL1DS
      .flatMap(entry => {
      entry.angles.map(a => {
        (entry.driveId, entry.driverId, a._1, a._2)
      })
    })

    driveLogDS = driveLogDS.join(distanceDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2) { (dl, r) => dl.distance = r._4; dl }

    driveLogDS = driveLogDS.join(distanceTotalDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2) { (dl, r) => dl.distanceTotal = r._4; dl }

    driveLogDS = driveLogDS.join(speedDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2) { (dl, r) => dl.speed = r._4; dl }

    driveLogDS = driveLogDS.join(accelerationDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2) { (dl, r) => dl.acceleration = r._4; dl }

    driveLogDS = driveLogDS.join(angleDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2) { (dl, r) => dl.angle = r._4; dl }

    (driveMeta, driveLogDS)
  }

}
