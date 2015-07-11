package com.peedeex21.axa.model;

/**
 * Created by peter on 08.07.15.
 */
public class DriveSample {

  public int driverId;
  public int driveId;
  public int seqNo;
  public double x;
  public double y;

  public DriveSample(int driverId, int driveId, int seqNo, double x, double y) {
    this.driverId = driverId;
    this.driveId = driveId;
    this.seqNo = seqNo;
    this.x = x;
    this.y = y;
  }

  @Override
  public String toString() {
    return driverId + "," + driveId + "," + seqNo + "," + x + "," + y;
  }

  public Vector2D toVector2D() {
    return new Vector2D(x, y);
  }

}
