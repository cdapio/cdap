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

package io.cdap.cdap.data2.dataset2;

import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.inmemory.MinimalTxSystemClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SingleThreadDatasetCache#close()} quiet closing of extra {@link TransactionAware}s.
 */
public class SingleThreadDatasetCacheUnitTest {

  @Test
  public void testCloseClosesAllCloseableExtraTxAwaresEvenWhenOneThrowsIoException()
      throws Exception {
    SystemDatasetInstantiator instantiator = Mockito.mock(SystemDatasetInstantiator.class);
    SingleThreadDatasetCache cache = new SingleThreadDatasetCache(
        instantiator,
        new MinimalTxSystemClient(),
        NamespaceId.DEFAULT,
        Collections.emptyMap(),
        null,
        null);

    final AtomicBoolean firstClosed = new AtomicBoolean(false);
    final AtomicBoolean secondClosed = new AtomicBoolean(false);

    CloseableTxAware first = new CloseableTxAware("first", firstClosed, false);
    CloseableTxAware secondThrows = new CloseableTxAware("second", secondClosed, true);
    TransactionAware nonCloseable = Mockito.mock(TransactionAware.class);

    // Note: addExtraTransactionAware uses addFirst (LIFO), so secondThrows runs BEFORE first!
    cache.addExtraTransactionAware(first);
    cache.addExtraTransactionAware(nonCloseable);
    cache.addExtraTransactionAware(secondThrows);

    cache.close();

    Assert.assertTrue(secondClosed.get());
    Assert.assertTrue("Remaining extraTxAware must still be closed after earlier IOException",
        firstClosed.get());
    Mockito.verify(instantiator).close();
  }

  private static final class CloseableTxAware implements TransactionAware, Closeable {
    private final String name;
    private final AtomicBoolean closed;
    private final boolean throwOnClose;

    CloseableTxAware(String name, AtomicBoolean closed, boolean throwOnClose) {
      this.name = name;
      this.closed = closed;
      this.throwOnClose = throwOnClose;
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      if (throwOnClose) {
        throw new IOException("close failure on " + name);
      }
    }

    @Override
    public void startTx(Transaction tx) {
      // no-op
    }

    @Override
    public void updateTx(Transaction tx) {
      // no-op
    }

    @Override
    public Collection<byte[]> getTxChanges() {
      return Collections.emptyList();
    }

    @Override
    public boolean commitTx() {
      return true;
    }

    @Override
    public void postTxCommit() {
      // no-op
    }

    @Override
    public boolean rollbackTx() {
      return true;
    }

    @Override
    public String getTransactionAwareName() {
      return name;
    }
  }
}
