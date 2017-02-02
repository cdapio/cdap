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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.logging.appender.AsyncLogAppender;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.file.FileLogAppender;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.logging.appender.standalone.StandaloneLogAppender;
import co.cask.cdap.logging.save.KafkaLogProcessor;
import co.cask.cdap.logging.save.KafkaLogProcessorFactory;
import co.cask.cdap.logging.save.LogMetricsPluginFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import java.util.HashSet;
import java.util.Set;

/**
 * Injectable modules for logging.
 */
public class LoggingModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogAppender.class).toProvider(LogAppenderProvider.class).in(Scopes.SINGLETON);
        Multibinder<KafkaLogProcessorFactory> handlerBinder = Multibinder.newSetBinder
          (binder(), KafkaLogProcessorFactory.class, Names.named(Constants.LogSaver.MESSAGE_PROCESSOR_FACTORIES));
        handlerBinder.addBinding().to(LogMetricsPluginFactory.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogAppender.class).toProvider(LogAppenderProvider.class).in(Scopes.SINGLETON);
        Multibinder<KafkaLogProcessorFactory> handlerBinder = Multibinder.newSetBinder
          (binder(), KafkaLogProcessorFactory.class, Names.named(Constants.LogSaver.MESSAGE_PROCESSOR_FACTORIES));
        handlerBinder.addBinding().to(LogMetricsPluginFactory.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogAppender.class).to(KafkaLogAppender.class);
      }
    };
  }

  /**
   * Provider for Async log appender and plugins to be used in standalone.
   */
  public static class LogAppenderProvider implements Provider<LogAppender> {
    private final LogAppender fileLogAppender;
    private final Set<KafkaLogProcessor> messageProcessors;

    @Inject
    public LogAppenderProvider(FileLogAppender fileLogAppender,
                               @Named(Constants.LogSaver.MESSAGE_PROCESSOR_FACTORIES)
                               Set<KafkaLogProcessorFactory> messageProcessorFactories) throws Exception {
      this.fileLogAppender = fileLogAppender;
      this.messageProcessors = new HashSet<>();
      for (KafkaLogProcessorFactory messageProcessorFactory : messageProcessorFactories) {
        messageProcessors.add(messageProcessorFactory.create());
      }
    }

    @Override
    public LogAppender get() {
      AsyncLogAppender asyncLogAppender = new AsyncLogAppender(fileLogAppender);
      return new StandaloneLogAppender(asyncLogAppender, messageProcessors);
    }
  }
}
