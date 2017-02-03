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

package co.cask.cdap.logging.framework;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public final class AppenderManager implements Iterable<Appender<ILoggingEvent>> {

  private static final int LOGGER_CACHE_MAX_SIZE = 500;
  private static final long LOGGER_CACHE_EXPIRY_MS = TimeUnit.MINUTES.toMillis(5);

  private final Set<Appender<ILoggingEvent>> defaultAppenders;
  private final Map<Logger, Set<Appender<ILoggingEvent>>> loggerAppenders;
  private final LoadingCache<String, Logger> effectiveLoggerCache;

  public AppenderManager(LoggerContext loggerContext) {
    this(loggerContext, Collections.<Appender<ILoggingEvent>>emptySet());
  }

  public AppenderManager(final LoggerContext loggerContext,
                         Iterable<? extends Appender<ILoggingEvent>> defaultAppenders) {

    this.defaultAppenders = Sets.newIdentityHashSet();
    Iterables.addAll(this.defaultAppenders, defaultAppenders);

    // Add allowed appenders for each defined logger in the logger context
    // If a logger is additive, the appenders added to its parents will also be added to the allowed list
    Map<Logger, Set<Appender<ILoggingEvent>>> loggerAppenders = new IdentityHashMap<>();
    for (Logger logger : loggerContext.getLoggerList()) {
      Set<Appender<ILoggingEvent>> allowedAppenders = Sets.newIdentityHashSet();
      loggerAppenders.put(logger, addAppenders(logger, allowedAppenders));
    }
    this.loggerAppenders = loggerAppenders;
    this.effectiveLoggerCache = CacheBuilder.newBuilder()
      .maximumSize(LOGGER_CACHE_MAX_SIZE)
      .expireAfterAccess(LOGGER_CACHE_EXPIRY_MS, TimeUnit.MILLISECONDS)
      .build(new CacheLoader<String, Logger>() {
        @Override
        public Logger load(String loggerName) throws Exception {
          return Loggers.getEffectiveLogger(loggerContext, loggerName);
        }
      });
  }

  @Override
  public Iterator<Appender<ILoggingEvent>> iterator() {
    return Iterables.concat(loggerAppenders.values()).iterator();
  }

  public LogEventFilter createLogEventFilter(final Appender<ILoggingEvent> appender) {
    // If the appender is a default appender, always log.
    if (defaultAppenders.contains(appender)) {
      return new LogEventFilter() {
        @Override
        public boolean apply(ILoggingEvent input) {
          return true;
        }
      };
    }

    // Otherwise, filter it by the effective log level and logger appender set.
    return new LogEventFilter() {
      @Override
      public boolean apply(ILoggingEvent event) {
        Logger logger = effectiveLoggerCache.getUnchecked(event.getLoggerName());
        return event.getLevel().isGreaterOrEqual(logger.getEffectiveLevel())
          && loggerAppenders.get(logger).contains(appender);
      }
    };
  }


  private <T extends Set<? super Appender<ILoggingEvent>>> T addAppenders(Logger logger, T appenders) {
    Iterator<Appender<ILoggingEvent>> iterator = logger.iteratorForAppenders();
    while (iterator.hasNext()) {
      appenders.add(iterator.next());
    }

    if (logger.isAdditive()) {
      Logger parent = Loggers.getParentLogger(logger);
      if (parent != null) {
        return addAppenders(parent, appenders);
      }
    }
    return appenders;
  }
}
