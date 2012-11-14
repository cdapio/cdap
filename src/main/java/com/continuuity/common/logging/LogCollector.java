package com.continuuity.common.logging;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class LogCollector {

  private static final Logger LOG
      = LoggerFactory.getLogger(LogCollector.class);

  ConcurrentMap<String, LogWriter> loggers = Maps.newConcurrentMap();

  private String pathPrefix;
  private FileSystem fs;

  public LogCollector(CConfiguration config) throws IOException {
    this.pathPrefix = config.get(Constants.CFG_LOG_COLLECTION_ROOT,
        Constants.DEFAULT_LOG_COLLECTION_ROOT);
    this.fs = FileSystem.get(config);
    LOG.info("Root directory for log collection is " + pathPrefix);
  }

  public void log(LogEvent event) {
    try {
      // get the logger for this tag and send the event to it
      getLogger(event.getTag()).log(event);
    } catch (IOException e) {
      // in case of error, log the error (but not the event) in the system log
      LOG.warn("Failed to log event for " +
          event.getTag() + ": " + e.getMessage(), e);
    }
  }

  private LogWriter getLogger(String tag) throws IOException {
    // figure out whether we already have a log writer for this tag
    LogWriter logger = loggers.get(tag);
    if (logger == null) {
      synchronized (this) {
        // check if it is stull null
        logger = loggers.get(tag);
        if (logger == null) {
          // create a new log configuration for this tag
          LogConfiguration conf =
              new LogConfiguration(this.fs, this.pathPrefix, tag);
          // create a new log writer
          logger = new LogFileWriter();
          logger.configure(conf);
          // remember this logger in the map
          loggers.put(tag, logger);
        }
      }
    }
    return logger;
  }

  public List<String> tail(String tag, int size) throws IOException {
    // create a new log configuration for this tag
    LogConfiguration conf =
        new LogConfiguration(this.fs, this.pathPrefix, tag);
    // create a new log reader
    LogReader reader = new LogFileReader();
    reader.configure(conf);
    return reader.tail(size);
  }

  public void close() throws IOException {
    for (Map.Entry<String, LogWriter> entry : loggers.entrySet()) {
      entry.getValue().close();
      loggers.remove(entry.getKey());
    }
  }

}
