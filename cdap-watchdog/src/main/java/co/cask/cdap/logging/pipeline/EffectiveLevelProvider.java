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

package co.cask.cdap.logging.pipeline;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.util.LoggerNameUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 *
 */
public final class EffectiveLevelProvider {

  private final LoadingCache<String, Level> levelCache;

  public EffectiveLevelProvider(final LoggerContext loggerContext, int maxCacheSize) {
    this.levelCache = CacheBuilder.newBuilder()
      .maximumSize(maxCacheSize)
      .build(new CacheLoader<String, Level>() {
        @Override
        public Level load(String loggerName) throws Exception {
          // If there is such logger, then just return the effective level
          Logger logger = loggerContext.exists(loggerName);
          if (logger != null) {
            return logger.getEffectiveLevel();
          }

          // Otherwise, search through the logger hierarchy. We don't call loggerContext.getLogger to avoid
          // caching of the logger inside the loggerContext, which there is no guarantee on the memory usage
          // and there is no way to clean it up.
          logger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
          int idx = LoggerNameUtil.getFirstSeparatorIndexOf(loggerName);
          while (idx >= 0) {
            Logger parentLogger = loggerContext.exists(loggerName.substring(0, idx));
            if (parentLogger != null) {
              logger = parentLogger;
            }
            idx = LoggerNameUtil.getSeparatorIndexOf(loggerName, idx + 1);
          }

          // If none is found, returns the effective level of the root logger
          return logger.getEffectiveLevel();
        }
      });
  }

  public Level getEffectiveLevel(ILoggingEvent event) {
    return getEffectiveLevel(event.getLoggerName());
  }

  @VisibleForTesting
  Level getEffectiveLevel(String loggerName) {
    return levelCache.getUnchecked(loggerName);
  }
}
