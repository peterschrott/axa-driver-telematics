package com.peedeex21.axa.io;

import com.peedeex21.axa.model.DriveSample;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * Created by peter on 08.07.15.
 */
public class AxaInputFormat2 extends DelimitedInputFormat<DriveSample> {

  private static final long serialVersionUID = 1L;

  /**
   * Code of \r, used to remove \r from a line when the line ends with \r\n
   */
  private static final byte CARRIAGE_RETURN = (byte) '\r';

  /**
   * Code of \n, used to identify if \n is used as delimiter
   */
  private static final byte NEW_LINE = (byte) '\n';


  /**
   * The name of the charset to use for decoding.
   */
  private String charsetName = "UTF-8";

  /**
   *
   */
  private HashMap<Integer, Integer> rowNumbers = new HashMap<>();

  // --------------------------------------------------------------------------------------------

  public AxaInputFormat2() {
    super();
    /* no splitting of the file */
    super.unsplittable = true;
  }

  // --------------------------------------------------------------------------------------------

  public String getCharsetName() {
    return charsetName;
  }

  public void setCharsetName(String charsetName) {
    if (charsetName == null) {
      throw new IllegalArgumentException("Charset must not be null.");
    }

    this.charsetName = charsetName;
  }

  // --------------------------------------------------------------------------------------------

  @Override
  public void configure(Configuration parameters) {
    /* recursive file read */
    parameters.setBoolean("recursive.file.enumeration", true);
    super.configure(parameters);

    if (charsetName == null || !Charset.isSupported(charsetName)) {
      throw new RuntimeException("Unsupported charset: " + charsetName);
    }
  }

  // --------------------------------------------------------------------------------------------

  @Override
  public DriveSample readRecord(DriveSample driveSample, byte[] bytes, int offset, int numBytes) throws IOException {
    //Check if \n is used as delimiter and the end of this line is a \r, then remove \r from the line
    if (this.getDelimiter() != null && this.getDelimiter().length == 1
        && this.getDelimiter()[0] == NEW_LINE && offset+numBytes >= 1
        && bytes[offset+numBytes-1] == CARRIAGE_RETURN){
      numBytes -= 1;
    }

    String line = new String(bytes, offset, numBytes, this.charsetName);
    String[] values = line.split(",");

    if(values[0].equals("x")) {
      // TODO this is not very nice + they must filtered out later
      return new DriveSample(-1, -1, -1, -1.0, -1.0);
    } else {
      String[] pathArray = super.currentSplit.getPath().toString().split("/");
      int driverId = Integer.parseInt(pathArray[pathArray.length - 2]);
      int driveId = Integer.parseInt(pathArray[pathArray.length - 1].split("\\.")[0]);

      int rowNumber = 0;
      int hashIdx = computeTableIndex(driverId, driveId);
      if (rowNumbers.get(hashIdx) != null){
        rowNumber = rowNumbers.get(hashIdx) + 1;
        rowNumbers.put(hashIdx, rowNumber);
      } else {
        rowNumbers.put(hashIdx, rowNumber);
      }

      return new DriveSample(driverId, driveId, rowNumber, Double.parseDouble(values[0]), Double.parseDouble(values[1]));
    }
  }

  private Integer computeTableIndex(int driverId, int driveId) {
    return driverId * 1000 + driveId;
  }

  // --------------------------------------------------------------------------------------------

  @Override
  public String toString() {
    return "AxaInputFormat (" + getFilePath() + ") - " + this.charsetName;
  }
}