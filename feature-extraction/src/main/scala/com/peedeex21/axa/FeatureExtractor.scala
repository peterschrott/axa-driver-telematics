package com.peedeex21.axa

import com.peedeex21.axa.model.{DriveMeta, Drive, DriveLog}
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

    val featureL2DS = featureL1DS.join(meansByDriverDrive)
      .where(d => (d.driverId, d.driveId)).equalTo(agg => (agg._1, agg._2))
      .map(product => {
        product._1.setSpeedMeanDriver(product._2._3/product._1.rowCount)
        product._1.setAccelerationMeanDriver(product._2._3/product._1.rowCount)
        product._1
      })

    // join the features with the original data set of x and
    // as join key the driver id, drive id and sequence number is used
    var driveLogDS = featureL2DS
      .flatMap(entry => {
        entry.coordinates.map(a => {
          new DriveLog(entry.driveId, entry.driverId, a._1, a._2.x, a._2.y)
        })
      })

    val distanceDS = featureL2DS
      .flatMap(entry => {
        entry.distances.map(a => {
          (entry.driveId, entry.driverId, a._1, a._2)
        })
      })

    val distanceTotalDS = featureL2DS
      .flatMap(entry => {
        entry.distanceTotal.map(a => {
          (entry.driveId, entry.driverId, a._1, a._2)
        })
      })

    val speedDS = featureL2DS
      .flatMap(entry => {
        entry.speeds.map(a => {
          (entry.driveId, entry.driverId, a._1, a._2)
        })
      })

    val accelerationDS = featureL2DS
      .flatMap(entry => {
        entry.accelerations.map(a => {
          (entry.driveId, entry.driverId, a._1, a._2)
        })
      })

    val angleDS = featureL2DS
      .flatMap(entry => {
        entry.angles.map(a => {
          (entry.driveId, entry.driverId, a._1, a._2)
        })
      })

    driveLogDS = driveLogDS.join(distanceDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
      {(dl, r) => dl.distance = r._4; dl}

    driveLogDS = driveLogDS.join(distanceTotalDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
      {(dl, r) => dl.distanceTotal = r._4; dl}

    driveLogDS = driveLogDS.join(speedDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
      {(dl, r) => dl.speed = r._4; dl}

    driveLogDS = driveLogDS.join(accelerationDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
      {(dl, r) => dl.acceleration = r._4; dl}

    driveLogDS = driveLogDS.join(angleDS).where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
      {(dl, r) => dl.angle = r._4; dl}

    /* join the features with the original data set of x and
       as join key the driver id, drive id and sequence number is used */
    /*val driveLogDS = featureL2DS.flatMap(drive => {
      val coordinateDS = env.fromCollection(drive.coordinates.toList)
      val distanceDS = env.fromCollection(drive.distances.toList)
      val distancesTotalDS = env.fromCollection(drive.distanceTotal.toList)
      val speedDs = env.fromCollection(drive.speeds.toList)
      val accelerationDS = env.fromCollection(drive.accelerations.toList)
      val angleDS = env.fromCollection(drive.angles.toList)

      val join = coordinateDS.map(c => {
          new DriveLog(drive.driverId, drive.driveId, c._1, c._2.x, c._2.y)
        })
        .join(distanceDS)
        .where(_.seqNo).equalTo(0) {(dl, r) => dl.distance = r._2; dl}
        .join(distancesTotalDS)
        .where(_.seqNo).equalTo(0) {(dl, r) => dl.distanceTotal = r._2; dl}
        .join(speedDs)
        .where(_.seqNo).equalTo(0) {(dl, r) => dl.speed = r._2; dl}
        .join(accelerationDS)
        .where(_.seqNo).equalTo(0) {(dl, r) => dl.acceleration = r._2; dl}
        .join(angleDS)
        .where(_.seqNo).equalTo(0) {(dl, r) => dl.angle = r._2; dl}

      join.collect()
    })*/

    val driveMeta = featureL1DS.map(entry => entry.transformToDriveMeta())

    (driveMeta, driveLogDS)
  }

}
