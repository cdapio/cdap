package com.continuuity.common.logging;

import java.io.IOException;

public class LogConfiguration {

  private final String account;
  private final String application;
  private final String flow;
  private final String prefix;
  private final String path;

  public final long DEFAULT_ROLL_THRESHOLD = 4 * 1024 * 1024; // 4MB
  public final int  DEFAULT_ROLL_INSTANCES = 5; // .log, log.1, ... log.4

  public LogConfiguration(String pathPrefix,
                          String tag) throws IOException {
    // parse the log tag
    String[] splits = tag.split(":");
    if (splits.length < 3)
      throw new IOException("Invalid log tag '" + tag + "'");
    String account = splits[0], app = splits[1], flow = splits[2];

    this.account = account;
    this.application = app;
    this.flow = flow;
    this.prefix = pathPrefix;
    this.path = String.format("%s/%s/%s/", pathPrefix, application, flow);
  }

  public String getLogFileName() {
    return "flow.log";
  }

  public String getLogFilePath() {
    return this.path;
  }

  public long getSizeThreshold() {
    return DEFAULT_ROLL_THRESHOLD;
  }

  public int getMaxInstances() {
    return DEFAULT_ROLL_INSTANCES;
  }

}
