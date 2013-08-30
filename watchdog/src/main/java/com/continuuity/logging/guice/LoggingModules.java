package com.continuuity.logging.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.logging.appender.LogAppender;
import com.continuuity.logging.appender.file.FileLogAppender;
import com.continuuity.logging.appender.kafka.KafkaLogAppender;
import com.continuuity.logging.read.DistributedLogReader;
import com.continuuity.logging.read.LogReader;
import com.continuuity.logging.read.SingleNodeLogReader;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 * Injectable modules for logging.
 */
public class LoggingModules extends RuntimeModule {
  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(SingleNodeLogReader.class);
        bind(LogAppender.class).to(FileLogAppender.class);
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(SingleNodeLogReader.class);
        bind(LogAppender.class).to(FileLogAppender.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(DistributedLogReader.class);
        bind(LogAppender.class).to(KafkaLogAppender.class);
      }
    };
  }
}
