/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.logging.pipeline;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Syncable;
import io.cdap.cdap.logging.framework.Loggers;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.Flushable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A context object provide to log processor pipeline.
 */
public class LogProcessorPipelineContext implements Flushable, Syncable, MetricsContext {

  private final String name;
  private final LoggerContext loggerContext;
  private final LoadingCache<String, Logger> effectiveLoggerCache;
  private final Set<Appender<ILoggingEvent>> appenders;
  private final String metricsPrefix;
  private final MetricsContext metricsContext;

  public LogProcessorPipelineContext(CConfiguration cConf, String name, final LoggerContext context,
                                     final MetricsContext metricsContext, int instanceId) {
    this.name = name;
    this.loggerContext = context;
    this.effectiveLoggerCache = CacheBuilder.newBuilder()
      .maximumSize(cConf.getInt(Constants.Logging.PIPELINE_LOGGER_CACHE_SIZE))
      .expireAfterAccess(cConf.getInt(Constants.Logging.PIPELINE_LOGGER_CACHE_EXPIRATION_MS), TimeUnit.MILLISECONDS)
      .build(new CacheLoader<String, Logger>() {
        @Override
        public Logger load(String loggerName) throws Exception {
          return Loggers.getEffectiveLogger(context, loggerName);
        }
      });

    // Grab all the appender instances in the context
    Set<Appender<ILoggingEvent>> appenders = Sets.newIdentityHashSet();
    for (Logger logger : context.getLoggerList()) {
      Iterators.addAll(appenders, logger.iteratorForAppenders());
    }
    this.appenders = appenders;
    Map<String, String> logTags = new HashMap<>();
    logTags.put(Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getNamespace());
    logTags.put(Constants.Metrics.Tag.COMPONENT, Constants.Service.LOGSAVER);
    this.metricsContext = metricsContext.childContext(logTags);
    this.metricsPrefix = String.format("%s.%s", name, instanceId);
  }

  /**
   * Return effective logger for the given logger name. The method caches the result from the call
   * {@link Loggers#getEffectiveLogger(LoggerContext, String)}.
   */
  public Logger getEffectiveLogger(String loggerName) {
    return effectiveLoggerCache.getUnchecked(loggerName);
  }

  /**
   * Returns the name of the pipeline.
   */
  public String getName() {
    return name;
  }

  /**
   * Flushes all appenders in this context. It will always try to flush all appenders even some might failed in between.
   *
   * @throws IOException if flushing of any appender failed. If more than one appender flush failed,
   *         the exception from the first appender failure will be thrown, with the subsequent failures
   *         added as suppressed exception
   */
  @Override
  public void flush() throws IOException {
    IOException failure = null;
    for (Appender<ILoggingEvent> appender : appenders) {
      if (appender instanceof Flushable) {
        try {
          ((Flushable) appender).flush();
        } catch (IOException e) {
          failure = addException(failure, e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Calls {@link Syncable#sync()} on all appenders in this context. It will always try to call sync on all
   * appenders event some might failed in between.
   *
   * @throws IOException if any appender failed to perform sync. If more than one appender sync failed,
   *         the exception from the first appender failure will be thrown, with the subsequent failures
   *         added as suppressed exception
   */
  @Override
  public void sync() throws IOException {
    IOException failure = null;
    for (Appender<ILoggingEvent> appender : appenders) {
      if (appender instanceof Syncable) {
        try {
          ((Syncable) appender).sync();
        } catch (IOException e) {
          failure = addException(failure, e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Starts this context.
   */
  public void start() {
    loggerContext.start();
  }

  /**
   * Stops this context.
   */
  public void stop() {
    loggerContext.stop();
  }

  /**
   * Adds the new exception to an existing exception. If there is no existing exception, simply return the
   * new exception.
   */
  private <E extends Exception> E addException(@Nullable E exception, E newException) {
    if (exception == null) {
      return newException;
    }
    exception.addSuppressed(newException);
    return exception;
  }

  @Override
  public MetricsContext childContext(Map<String, String> tags) {
    return metricsContext.childContext(tags);
  }

  @Override
  public MetricsContext childContext(String tagName, String tagValue) {
    return metricsContext.childContext(tagName, tagValue);
  }

  @Override
  public Map<String, String> getTags() {
    return metricsContext.getTags();
  }

  @Override
  public void increment(String metricName, long value) {
    metricsContext.increment(String.format("%s.%s", metricsPrefix, metricName), value);
  }

  @Override
  public void gauge(String metricName, long value) {
    metricsContext.gauge(String.format("%s.%s", metricsPrefix, metricName), value);
  }

  @Override
  public void event(String metricName, long value) {
    metricsContext.event(String.format("%s.%s", metricsPrefix, metricName), value);
  }
}
