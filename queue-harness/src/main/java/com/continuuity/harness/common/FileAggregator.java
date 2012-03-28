/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 */
package com.continuuity.harness.common;

import etm.contrib.aggregation.log.AbstractLogAggregator;
import etm.core.aggregation.Aggregator;
import etm.core.metadata.AggregatorMetaData;
import etm.core.monitor.EtmPoint;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * FileAggregator dumps each data point to the file. The name of each point,
 * start time, end time, and time it took to finish the operation in nano-seconds.
 * A filename is provided. If a IOException or File is not found the results will
 * be skipped after that point on. We don't want to keep trying as that might
 * affect the measurement.
 */
public class FileAggregator extends AbstractLogAggregator  {
  private static final String DESCRIPTION = "Logs to file";
  private String filename;
  private FileOutputStream fileOutputStream = null;
  private boolean streamGood;

  /**
   * Initialization
   * @param aggregator Aggregator
   * @param filename   Name of the file to which data points are returned
   */
  public FileAggregator(Aggregator aggregator, String filename) {
    super(aggregator);
    this.filename = filename;
    this.streamGood = true;
  }

  /**
   * Logs a data point that is captured. The points are buffered and streamed
   * to this class for logging.
   *
   * @param point   A point captured
   */
  protected void logMeasurement(EtmPoint point) {
    if(fileOutputStream == null) {
      try {
        fileOutputStream = new FileOutputStream(filename);
      }  catch (FileNotFoundException e) {
        streamGood = false;
      }
    }

    if(!streamGood) return;

    StringBuffer sb = new StringBuffer();
    sb.append(String.format("%-20d", point.getStartTime()));
    sb.append("\t");
    sb.append(String.format("%-20d",point.getEndTime()));
    sb.append("\t");
    sb.append(String.format("%10d", point.getEndTime() - point.getStartTime()));
    sb.append("\t");
    sb.append(String.format("%20s", point.getName()));
    sb.append("\n");

    try {
      fileOutputStream.write(sb.toString().getBytes());
    } catch (IOException e) {
      streamGood = false;
    }
  }

  /**
   * Meta Data of the aggregator
   * @return
   */
  public AggregatorMetaData getMetaData() {
    return new
      AggregatorMetaData(FileAggregator.class, DESCRIPTION,
          false, delegate.getMetaData());
  }

  /**
   * Starts the aggregator
   */
  public void start() {
    super.start();
  }

  /**
   * Stops the aggregator
   */
  public void stop() {
    try {
      if(fileOutputStream != null) fileOutputStream.close();
    } catch(IOException e) {

    }
    super.stop();
  }
}
