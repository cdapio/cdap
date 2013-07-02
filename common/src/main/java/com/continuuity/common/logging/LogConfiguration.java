package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Configuration for logging.
 */
public class LogConfiguration {

  private final String account;
  private final String application;
  private final String flow;
  private final String prefix;
  private final String path;
  private final FileSystem fs;
  private final long threshold;
  private final int instances;


  public static final String CFG_ROLL_THRESHOLD = "logfile.roll.size";
  public static final String CFG_ROLL_INSTANCES = "logfile.roll.instances";

  public static final long DEFAULT_ROLL_THRESHOLD = 4 * 1024 * 1024; // 4MB
  public static final int  DEFAULT_ROLL_INSTANCES = 5; // .log, log.1, ... log.4

  public LogConfiguration(FileSystem fs,
                          CConfiguration config,
                          String pathPrefix,
                          String tag) throws IOException {
    // parse the log tag
    String[] splits = tag.split(":");
    if (splits.length < 3) {
      throw new IOException("Invalid log tag '" + tag + "'");
    }
    String account = splits[0], app = splits[1], flow = splits[2];

    this.fs = fs;
    this.account = account;
    this.application = app;
    this.flow = flow;
    this.prefix = pathPrefix;
    this.path = String.format("%s/%s/%s/", pathPrefix, application, flow);

    this.threshold = config.getLong(CFG_ROLL_THRESHOLD, DEFAULT_ROLL_THRESHOLD);
    this.instances = config.getInt(CFG_ROLL_INSTANCES, DEFAULT_ROLL_INSTANCES);
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  public String getLogFileName() {
    return "flow.log";
  }

  public String getLogFilePath() {
    return this.path;
  }

  public long getSizeThreshold() {
    return this.threshold;
  }

  public int getMaxInstances() {
    return this.instances;
  }
}
