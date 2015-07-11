package com.peedeex21.axa.model

/**
 * Created by peter on 07.07.15.
 */
class DriveFeatures {}

object DriveFeatures {

  val pojoFields = Array("driverId", "driveId", "feat1", "feat2", "feat3", "feat4", "feat5",
    "feat6", "feat7", "feat8", "feat9", "feat10", "feat11", "feat12", "feat13", "feat14",
    "feat15", "feat16", "feat17", "feat18", "feat19", "feat20", "feat21", "feat22", "feat23",
    "feat24", "feat25", "feat26", "feat27", "feat28", "feat29", "feat30", "feat31", "feat32",
    "feat33", "feat34", "feat35", "feat36", "feat37", "feat38", "feat39", "feat40", "feat41",
    "feat42", "feat43", "feat44", "feat45", "feat46", "feat47", "feat48", "feat49", "feat50",
    "feat51", "feat52", "feat53", "feat54", "feat55", "feat56", "feat57", "feat58", "feat59",
    "feat60", "feat61", "feat62", "feat63", "feat64", "feat65", "feat66", "feat67", "feat68",
    "feat69", "feat70", "feat71", "feat72")

  var nonFeatures = Array("driverId", "driveId")

  var features = this.pojoFields.diff(this.nonFeatures)

}
