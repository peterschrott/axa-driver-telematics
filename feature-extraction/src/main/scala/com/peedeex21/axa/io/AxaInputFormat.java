package com.peedeex21.axa.io;

import com.peedeex21.axa.model.Drive;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.StringWriter;

/**
 * !!! FOR NOW THIS DOES NOT WORK IN PARALLEL EXECUTION !!
 *
 * This is a simple input format for the data set of the Kaggle competition AXA Driver Telematics.
 *
 * The structure of the data set:
 *
 * folder1 (driver)
 *       +-----file1 (drive)
 *                 +-----column x
 *                 +-----column y
 *
 * There is a folder for each driver (1 - 2736). For each driver there are the logged drives in
 * CSV format (1 - 200). Each drive log contains the anonymized x- / y- coordinates of the drive,
 * sampled every second.
 *
 * Created by Peter Schrott on 05.06.15.
 */
public class AxaInputFormat extends FileInputFormat<Drive> {

  /* This is a simple hack to read in whole files at once.
   * The boolean is toggled each time [[reachedEnd()]] is called.
   * This is at the begin of the file read and at the end again. */
  private boolean end = false;

  public AxaInputFormat() {
    super();
    /* no splitting of the file */
    super.unsplittable = false;
  }

  @Override
  public void configure(Configuration parameters) {
    /* recursive file read */
    parameters.setBoolean("recursive.file.enumeration", true);
    super.configure(parameters);
  }

  public Drive nextRecord(Drive reuse) throws IOException {
    /* read the file content */
    String content = readFileContent();
    /* extract the information regarding folder structure */
    String[] pathArray = super.currentSplit.getPath().toString().split("/");
    int driverId = Integer.parseInt(pathArray[pathArray.length-2]);
    int driveId = Integer.parseInt(pathArray[pathArray.length-1].split("\\.")[0]);
    /* return the POJO Drive */
    return new Drive(driverId, driveId, content);
  }

  public boolean reachedEnd() throws IOException {
    boolean end_prev = end;
    end = !end;
    return end_prev;
  }

  private String readFileContent() throws IOException{
    StringWriter writer = new StringWriter();
    IOUtils.copy(super.stream, writer, "UTF-8");
    return writer.toString();
  }

}
