package com.peedeex21.axa.model

/**
 * Created by peter on 12.07.15.
 */
class DriveFeatures2 {}

object DriveFeatures2 {

  val pojoFields = Array("driverId", "driveId", "avg_speed", "sd_speed", "min_speed", "max_speed",
    "q1_speed", "q2_speed", "q3_speed", "avg_acc", "sd_acc", "min_acc", "max_acc", "q1_acc",
    "q2_acc", "q3_acc", "avg_dist_to_orig", "sd_dist_to_orig", "max_dist_to_orig",
    "q1_dist_to_orig", "q2_dist_to_orig", "q3_dist_to_orig", "acc_time", "dec_time",
    "trip_duration", "crow_flight", "trip_length", "crow_ratio", "is_internal")

  var nonFeatures = Array("driverId", "driveId")

  var features = this.pojoFields.diff(this.nonFeatures)

}

