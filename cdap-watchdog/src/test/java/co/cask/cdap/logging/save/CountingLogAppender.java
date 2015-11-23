/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts number of log messages being appended per log partition.
 */
public class CountingLogAppender extends LogAppender {
  private final LoadingCache<String, AtomicLong> counterCache =
    CacheBuilder.newBuilder().build(new CacheLoader<String, AtomicLong>() {
      @Override
      public AtomicLong load(@SuppressWarnings("NullableProblems") String key) throws Exception {
        return new AtomicLong(0);
      }
    });
  private final LogAppender logAppender;

  public CountingLogAppender(LogAppender logAppender) {
    this.logAppender = logAppender;
  }

  @Override
  protected void append(LogMessage logMessage) {
    try {
      counterCache.get(logMessage.getLoggingContext().getLogPartition()).incrementAndGet();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    logAppender.append(logMessage);
  }

  public long getCount(String logPartition) throws ExecutionException {
    return counterCache.get(logPartition).get();
  }
}
