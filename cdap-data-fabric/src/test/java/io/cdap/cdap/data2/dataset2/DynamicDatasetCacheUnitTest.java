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

import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.inmemory.MinimalTxSystemClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DynamicDatasetCache} closeQuietly and {@code DatasetCacheKey.toString()}.
 */
public class DynamicDatasetCacheUnitTest {

  @Test
  public void testCloseSwallowsInstantiatorIoExceptionAndDatasetCacheKeyToString()
      throws Exception {
    final AtomicBoolean instantiatorCloseCalled = new AtomicBoolean(false);
    SystemDatasetInstantiator instantiator = Mockito.mock(SystemDatasetInstantiator.class);
    Mockito.doAnswer(inv -> {
      instantiatorCloseCalled.set(true);
      throw new IOException("instantiator close failure");
    }).when(instantiator).close();

    DynamicDatasetCache.DatasetCacheKey key = new DynamicDatasetCache.DatasetCacheKey(
        "ns1", "myTable", Collections.singletonMap("k", "v"), AccessType.READ_WRITE);
    Assert.assertEquals(
        "DatasetCacheKey{namespace=ns1, name=myTable, arguments={k=v}, accessType=READ_WRITE}",
        key.toString());

    DynamicDatasetCache cache = new MinimalDynamicDatasetCache(instantiator);
    cache.close();
    Assert.assertTrue(instantiatorCloseCalled.get());
  }

  private static final class MinimalDynamicDatasetCache extends DynamicDatasetCache {
    MinimalDynamicDatasetCache(SystemDatasetInstantiator instantiator) {
      super(instantiator, new MinimalTxSystemClient(), NamespaceId.DEFAULT, Collections.emptyMap());
    }

    @Override
    protected <T extends Dataset> T getDataset(DatasetCacheKey key, boolean bypass) {
      return null;
    }

    @Override
    public TransactionContext newTransactionContext() {
      return null;
    }

    @Override
    public void dismissTransactionContext() {
      // no-op
    }

    @Override
    public Iterable<TransactionAware> getStaticTransactionAwares() {
      return Collections.emptyList();
    }

    @Override
    public Iterable<TransactionAware> getTransactionAwares() {
      return Collections.emptyList();
    }

    @Override
    public Iterable<TransactionAware> getExtraTransactionAwares() {
      return Collections.emptyList();
    }

    @Override
    public void addExtraTransactionAware(TransactionAware txAware) {
      // no-op
    }

    @Override
    public void removeExtraTransactionAware(TransactionAware txAware) {
      // no-op
    }

    @Override
    public void invalidate() {
      // no-op
    }

    @Override
    public void discardDataset(Dataset dataset) {
      // no-op
    }
  }
}
