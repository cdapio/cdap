/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging.common;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LogCollector;
import com.continuuity.common.logging.LogEvent;
import org.apache.hadoop.conf.Configuration;

/**
 * Implementation of LogWriter that writes to log collector directly.
 */
public class LocalLogWriter implements LogWriter {
  private final LogCollector collector;

  public LocalLogWriter(CConfiguration configuration) {
    Configuration hConfiguration = new Configuration();
    this.collector = new LogCollector(configuration, hConfiguration);
  }

  @Override
  public boolean write(final String tag, final String level, final String message) {
    LogEvent event = new LogEvent(tag, level, message);
    collector.log(event);
    return true;
  }
}
