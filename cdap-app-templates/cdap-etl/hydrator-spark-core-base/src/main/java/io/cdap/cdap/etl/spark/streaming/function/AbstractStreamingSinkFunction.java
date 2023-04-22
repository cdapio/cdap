/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.streaming.function;

import io.cdap.cdap.api.retry.RetryFailedException;
import io.cdap.cdap.etl.spark.streaming.StreamingRetrySettings;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class implementing {@link VoidFunction2} that retries the function call based on
 * {@link StreamingRetrySettings}
 *
 * @param <S>
 */
public abstract class AbstractStreamingSinkFunction<S> implements VoidFunction2<S, Time> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamingSinkFunction.class);
  private final long maxRetryTimeInMillis;
  private final long baseDelayInMillis;
  private final long maxDelayInMillis;
  private final SerializableCallable batchRetryFunction;

  protected AbstractStreamingSinkFunction(StreamingRetrySettings streamingRetrySettings,
      SerializableCallable batchRetryFunction) {
    this.maxRetryTimeInMillis = TimeUnit.MILLISECONDS.convert(streamingRetrySettings.getMaxRetryTimeInMins(),
                                                              TimeUnit.MINUTES);
    this.baseDelayInMillis = TimeUnit.MILLISECONDS.convert(streamingRetrySettings.getBaseDelayInSeconds(),
                                                           TimeUnit.SECONDS);
    this.maxDelayInMillis = TimeUnit.MILLISECONDS.convert(streamingRetrySettings.getMaxDelayInSeconds(),
                                                          TimeUnit.SECONDS);
    this.batchRetryFunction = batchRetryFunction;
    LOG.info("Retry settings in millis : {} , {} ,  {} ", maxRetryTimeInMillis, baseDelayInMillis, maxDelayInMillis);
  }

  @Override
  public void call(S v1, Time time) throws Exception {
    int attempt = 1;
    long startTimeInMillis = System.currentTimeMillis();
    while (true) {
      try {
        if (attempt > 1) {
          batchRetryFunction.call();
        }
        retryableCall(v1, time);
        LOG.debug("Returning successfully.");
        return;
      } catch (Exception e) {
        long delay = getDelay(maxDelayInMillis, baseDelayInMillis, attempt);
        if (System.currentTimeMillis() - startTimeInMillis > maxRetryTimeInMillis) {
          LOG.info("Exceeded maximum retry time limit.");
          throw e;
        }
        LOG.warn("Exception thrown while executing runnable with sinks {} on attempt {}. Retrying in {} ms.",
                 getSinkNames(), attempt++, delay, e);
        try {
          TimeUnit.MILLISECONDS.sleep(delay);
        } catch (InterruptedException ie) {
          e.addSuppressed(ie);
          Thread.currentThread().interrupt();
          e.addSuppressed(new RetryFailedException("Retry failed. Thread got interrupted.", attempt));
          throw e;
        }
      }
    }
  }

  private long getDelay(long maxDelayInMillis, long baseDelayInMillis, int attempt) {
    long power = attempt > Long.SIZE ? Long.MAX_VALUE : (1L << attempt - 1);
    long delay = Math.min(baseDelayInMillis * power, maxDelayInMillis);
    delay = delay < 0 ? maxDelayInMillis : delay;
    return delay;
  }

  /**
   * For the sub classes to override
   *
   * @param v1 Type parameter that the subclass is using
   * @param v2 Time for the batch
   * @throws Exception
   */
  protected abstract void retryableCall(S v1, Time v2) throws Exception;

  /**
   * Sinks used in this function
   * @return
   */
  protected abstract Set<String> getSinkNames();
}
