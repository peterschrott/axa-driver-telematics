package com.peedeex21.axa.model;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by Peter Schrott on 05.06.15.
 */
public class AxaInputFormat extends FileInputFormat<Drive> {

  private boolean end = false;

  public AxaInputFormat() {
    super();
    super.unsplittable = false;
  }

  @Override
  public void configure(Configuration parameters) {
    parameters.setBoolean("recursive.file.enumeration", true);
    super.configure(parameters);
  }

  public Drive nextRecord(Drive reuse) throws IOException {
    int driverId = 1;
    int driveId = 1;
    String content = readFileContent();
    return new Drive(driverId, driveId, content);
  }

  public boolean reachedEnd() throws IOException {
    boolean end_prev = end;
    end = !end;
    return end_prev;
  }

  private String readFileContent() {
    StringWriter writer = new StringWriter();
    try {
      IOUtils.copy(super.stream, writer, "UTF-8");
    } catch (IOException e) {
      e.printStackTrace();
      return "";
    }
    return writer.toString();
  }

}
