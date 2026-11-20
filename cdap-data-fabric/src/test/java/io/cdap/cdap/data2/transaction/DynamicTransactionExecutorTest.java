/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.transaction;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionAwares;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.inmemory.MinimalTxSystemClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link DynamicTransactionExecutor}.
 */
public class DynamicTransactionExecutorTest {

  @Test
  public void testSameThreadExecutionSubmitRetryAndAbort() throws Exception {
    MinimalTxSystemClient txClient = new MinimalTxSystemClient();
    TransactionAware emptyTxAware = TransactionAwares.of(Collections.emptyList());
    TransactionContextFactory factory =
        () -> new TransactionContext(txClient, emptyTxAware);

    DynamicTransactionExecutor executor =
        new DynamicTransactionExecutor(factory, RetryStrategies.retryOnConflict(2, 0));

    final Thread callerThread = Thread.currentThread();
    String out = executor.execute(() -> {
      Assert.assertSame(callerThread, Thread.currentThread());
      return "ok";
    });
    Assert.assertEquals("ok", out);

    // Exercise AbstractTransactionExecutor.submit(...) backed by MoreExecutors.sameThreadExecutor()
    Integer futureOut = executor.submit((TransactionExecutor.Function<Integer, Integer>) x -> {
      Assert.assertSame(callerThread, Thread.currentThread());
      return x * 3;
    }, 7).get();
    Assert.assertEquals(Integer.valueOf(21), futureOut);

    // Verify retry on TransactionConflictException during commit
    AtomicInteger finishAttempts = new AtomicInteger();
    TransactionContextFactory conflictOnceFactory = () -> new TransactionContext(txClient, emptyTxAware) {
      @Override
      public void finish() throws TransactionFailureException {
        if (finishAttempts.incrementAndGet() == 1) {
          super.abort();
          throw new TransactionConflictException(1L, "conflictKey", "conflictWriter");
        }
        super.finish();
      }
    };
    DynamicTransactionExecutor retryExecutor =
        new DynamicTransactionExecutor(conflictOnceFactory, RetryStrategies.retryOnConflict(2, 0));
    AtomicInteger fnRuns = new AtomicInteger();
    String retried = retryExecutor.execute(() -> {
      fnRuns.incrementAndGet();
      return "recovered";
    });
    Assert.assertEquals("recovered", retried);
    Assert.assertEquals(2, fnRuns.get());

    // Verify failure aborts and wraps cause in TransactionFailureException
    final IllegalStateException expectedCause = new IllegalStateException("fn failed");
    try {
      executor.execute((TransactionExecutor.Subroutine) () -> {
        throw expectedCause;
      });
      Assert.fail("Expected TransactionFailureException");
    } catch (TransactionFailureException actual) {
      Assert.assertSame(expectedCause, actual.getCause());
    }
  }
}
