/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner;

import com.google.api.client.util.ExponentialBackOff;
import com.google.common.base.Throwables;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retries Cloud Spanner operations in case they fail due to retryable errors.
 */
public class RetryingSpannerTransactionRunner implements TransactionRunner {

  private static final Logger LOG = LoggerFactory.getLogger(RetryingSpannerTransactionRunner.class);
  static final String MAX_RETRIES = "tx.runner.max.retries";
  static final String INITIAL_DELAY_MILLIS = "tx.runner.initial.delay.ms";
  private final int maxRetries;
  private final int initialDelayMs;
  private final SpannerTransactionRunner transactionRunner;

  RetryingSpannerTransactionRunner(Map<String, String> conf, SpannerStructuredTableAdmin admin) {
    this.maxRetries = Integer.parseInt(conf.get(MAX_RETRIES));
    this.initialDelayMs = Integer.parseInt(conf.get(INITIAL_DELAY_MILLIS));
    this.transactionRunner = new SpannerTransactionRunner(admin);
  }

  @Override
  public void run(TxRunnable runnable) throws TransactionException {
    ExponentialBackOff backOff = new ExponentialBackOff.Builder().setInitialIntervalMillis(
        initialDelayMs).build();
    int counter = 0;
    TransactionException exception = null;
    while (counter < maxRetries) {
      counter++;
      try {
        transactionRunner.run(runnable);
        return;
      } catch (TransactionException e) {
        exception = e;
        if (isRetryable(e)) {
          LOG.debug("Transaction failed with retryable exception", e);
          try {
            Thread.sleep(backOff.nextBackOffMillis());
          } catch (InterruptedException | IOException ex) {
            // Reinstate the interrupt
            Thread.currentThread().interrupt();
            // Fail with the original exception
            throw e;
          }
        } else {
          throw e;
        }
      }
    }
    throw exception;
  }

  private boolean isRetryable(TransactionException e) {
    List<Throwable> causes = Throwables.getCausalChain(e);
    for (Throwable cause : causes) {
      if (cause instanceof TableNotFoundException) {
        return true;
      }
    }
    return false;
  }
}
