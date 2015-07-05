package com.peedeex21.axa

import com.peedeex21.axa.model.{Drive, DriveLog, DriveMeta}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order

case class DriverMeans(driverId: Int, count: Int, speedMean: Double, accMean: Double)

case class DriverSdBase(driverId: Int, count: Int, speedMean: Double, speedMeanDriver: Double,
                        accMean: Double, accMeanDriver: Double)
case class DriverSd(driverId: Int, count: Int, speedSd: Double, accSd: Double)


/**
 * Created by Peter Schrott
 */
class FeatureExtractor() {

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

    /*
     * level 2 features:
     * - speedMean per driver
     * - accelerationMean per driver
     * - speed standard deviation per driver
     * - acceleration standard deviation per driver
     */
    val driverMeans =
      featureL1DS.map(d => new DriverMeans(d.driverId, 1, d.speedMean, d.accMean))
        .groupBy(_.driverId)
        .reduce((dl, dr) =>
          new DriverMeans(dl.driverId, (dl.count+dr.count),
            (dl.speedMean + dr.speedMean), (dl.accMean + dr.accMean)))
        .map(d => {
          new DriverMeans(d.driverId, d.count, d.speedMean/d.count, d.accMean/d.count)
        })
    
    val featureL2DS_tmp = featureL1DS.join(driverMeans)
        .where(_.driverId).equalTo(_.driverId)
        .map(join => {
          join._1.setSpeedMeanDriver(join._2.speedMean)
          join._1.setAccMeanDriver(join._2.accMean)
          join._1
        })

    val driverSd = featureL2DS_tmp.map(d =>
          new DriverSd(d.driverId, 1,  math.pow(d.speedMeanDeviation, 2), math.pow(d.accMeanDeviation, 2)))
        .groupBy(_.driverId)
        .reduce((dl, dr) =>
          new DriverSd(dl.driverId, (dl.count+dr.count), (dl.speedSd+dr.speedSd), (dl.accSd + dr.accSd)))
        .map(d => {
          new DriverSd(d.driverId, d.count, math.sqrt(d.speedSd/d.count), math.sqrt(d.accSd/d.count))
        })

    val featureL2DS = featureL2DS_tmp.join(driverSd)
        .where(_.driverId).equalTo(_.driverId)
        .map(join => {
          join._1.setSpeedSdDriver(join._2.speedSd)
          join._1.setAccSdDriver(join._2.accSd)
          join._1
        })

    val driveMeta = featureL2DS.map(entry => entry.transformToDriveMeta)

    // join the features with the original data set of x and
    // as join key the driver id, drive id and sequence number is used
    var driveLogDS = featureL1DS
      .flatMap(entry => {
      entry.coordinates.map(a => {
        new DriveLog(entry.driverId, entry.driveId, a._1, a._2.x, a._2.y)
      })
    })

    val distanceDS = featureL1DS
      .flatMap(entry => {
      entry.distanceDeltaMap.map(a => {
        (entry.driverId, entry.driveId, a._1, a._2)
      })
    })

    val distanceTotalDS = featureL1DS
      .flatMap(entry => {
      entry.distanceTotalMap.map(a => {
        (entry.driverId, entry.driveId, a._1, a._2)
      })
    })

    val speedDS = featureL1DS
      .flatMap(entry => {
      entry.speedDeltaMap.map(a => {
        (entry.driverId, entry.driveId, a._1, a._2)
      })
    })

    val accelerationDS = featureL1DS
      .flatMap(entry => {
      entry.accDeltaMap.map(a => {
        (entry.driverId, entry.driveId, a._1, a._2)
      })
    })

    val angleDS = featureL1DS
      .flatMap(entry => {
      entry.angleDeltaMap.map(a => {
        (entry.driverId, entry.driveId, a._1, a._2)
      })
    })

    driveLogDS = driveLogDS.join(distanceDS)
      .where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
        { (dl, r) => dl.distanceDelta = r._4; dl }

    driveLogDS = driveLogDS.join(distanceTotalDS)
      .where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
        { (dl, r) => dl.distanceTotal = r._4; dl }

    driveLogDS = driveLogDS.join(speedDS)
      .where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
        { (dl, r) => dl.speedDelta = r._4; dl }

    driveLogDS = driveLogDS.join(accelerationDS)
      .where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
        { (dl, r) => dl.accelerationDelta = r._4; dl }

    driveLogDS = driveLogDS.join(angleDS)
      .where(dl => (dl.driverId, dl.driveId, dl.seqNo)).equalTo(0, 1, 2)
        { (dl, r) => dl.angleDelta = r._4; dl }

    driveLogDS.sortPartition("seqNo", Order.ASCENDING)

    (driveMeta, driveLogDS)
  }

}
