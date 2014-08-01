/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
