/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.logging.guice;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.logging.appender.AsyncLogAppender;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.file.FileLogAppender;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.logging.read.DistributedLogReader;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.read.SingleNodeLogReader;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

/**
 * Injectable modules for logging.
 */
public class LoggingModules extends RuntimeModule {
  private static final String SYNC_LOG_APPENDER_ANNOTATION = "sync.log.appender";

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(SingleNodeLogReader.class);
        bind(LogAppender.class).annotatedWith(Names.named(SYNC_LOG_APPENDER_ANNOTATION)).to(FileLogAppender.class);
        bind(LogAppender.class).toProvider(AsyncLogAppenderProvider.class).in(Scopes.SINGLETON);
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(SingleNodeLogReader.class);
        bind(LogAppender.class).annotatedWith(Names.named(SYNC_LOG_APPENDER_ANNOTATION)).to(FileLogAppender.class);
        bind(LogAppender.class).toProvider(AsyncLogAppenderProvider.class).in(Scopes.SINGLETON);
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

  /**
   * Provider of async LogAppender.
   */
  public static class AsyncLogAppenderProvider implements Provider<LogAppender> {
    private final LogAppender logAppender;

    @Inject
    public AsyncLogAppenderProvider(@Named(SYNC_LOG_APPENDER_ANNOTATION) LogAppender logAppender) {
      this.logAppender = logAppender;
    }

    @Override
    public LogAppender get() {
      return new AsyncLogAppender(logAppender);
    }
  }
}
