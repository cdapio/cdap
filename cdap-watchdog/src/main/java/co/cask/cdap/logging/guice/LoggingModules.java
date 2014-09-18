/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.AsyncLogAppender;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.file.FileLogAppender;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.logging.read.DistributedLogReader;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.logging.read.StandaloneLogReader;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

/**
 * Injectable modules for logging.
 */
public class LoggingModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(StandaloneLogReader.class);
        bind(LogAppender.class).annotatedWith(Names.named(LoggingConfiguration.SYNC_LOG_APPENDER_ANNOTATION))
          .to(FileLogAppender.class).in(Scopes.SINGLETON);
        bind(LogAppender.class).to(AsyncLogAppender.class).in(Scopes.SINGLETON);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(StandaloneLogReader.class);
        bind(LogAppender.class).annotatedWith(Names.named(LoggingConfiguration.SYNC_LOG_APPENDER_ANNOTATION))
          .to(FileLogAppender.class).in(Scopes.SINGLETON);
        bind(LogAppender.class).to(AsyncLogAppender.class).in(Scopes.SINGLETON);
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
