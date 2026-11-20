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

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.tephra.inmemory.MinimalTxSystemClient;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Fast deterministic unit tests for {@link MultiThreadDatasetCache}.
 */
public class MultiThreadDatasetCacheUnitTest {

  @Test
  public void testPerThreadIsolationAndClose() throws Exception {
    SystemDatasetInstantiator instantiator = Mockito.mock(SystemDatasetInstantiator.class);
    Mockito.when(instantiator.getDataset(Mockito.any(DatasetId.class), Mockito.anyMap(), Mockito.any()))
        .thenAnswer(inv -> Mockito.mock(Dataset.class));

    MultiThreadDatasetCache cache = new MultiThreadDatasetCache(
        instantiator,
        new MinimalTxSystemClient(),
        NamespaceId.DEFAULT,
        Collections.emptyMap(),
        null,
        null);

    Dataset mainDs = cache.getDataset("ds");
    final AtomicReference<Dataset> workerDsRef = new AtomicReference<>();
    Thread worker = new Thread(() -> workerDsRef.set(cache.getDataset("ds")));
    worker.start();
    worker.join();

    Assert.assertNotNull(mainDs);
    Assert.assertNotNull(workerDsRef.get());
    Assert.assertNotSame(mainDs, workerDsRef.get());
    Assert.assertEquals(2, cache.getCacheKeys().size());

    cache.invalidate();
    cache.close();
    // Each of the 2 per-thread SingleThreadDatasetCaches closes instantiator on invalidation, plus super.close()
    Mockito.verify(instantiator, Mockito.times(3)).close();
  }

  @Test
  public void testStaticDatasetLoadFailurePropagatesThroughPerThreadCache() {
    SystemDatasetInstantiator instantiator = Mockito.mock(SystemDatasetInstantiator.class);
    final DatasetInstantiationException expectedEx =
        new DatasetInstantiationException("missing static dataset");
    Mockito.when(instantiator.getDataset(Mockito.any(DatasetId.class), Mockito.anyMap(), Mockito.any()))
        .thenThrow(expectedEx);

    Map<String, Map<String, String>> staticDatasets =
        Collections.singletonMap("missingStatic", Collections.emptyMap());
    MultiThreadDatasetCache cache = new MultiThreadDatasetCache(
        instantiator,
        new MinimalTxSystemClient(),
        NamespaceId.DEFAULT,
        Collections.emptyMap(),
        null,
        staticDatasets);

    try {
      cache.getStaticTransactionAwares();
      Assert.fail("Expected UncheckedExecutionException");
    } catch (UncheckedExecutionException actual) {
      Assert.assertTrue(actual.getCause() instanceof DatasetInstantiationException);
      Assert.assertTrue(actual.getCause().getCause() instanceof UncheckedExecutionException);
      Assert.assertSame(expectedEx, actual.getCause().getCause().getCause());
    } finally {
      cache.close();
    }
  }
}
