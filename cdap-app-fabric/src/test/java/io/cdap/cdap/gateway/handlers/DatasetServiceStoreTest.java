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

package io.cdap.cdap.gateway.handlers;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.kv.NoTxKeyValueTable;
import io.cdap.cdap.proto.RestartServiceInstancesStatus;
import io.cdap.cdap.proto.RestartServiceInstancesStatus.RestartStatus;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DatasetServiceStore}.
 */
public class DatasetServiceStoreTest {

  @Test
  public void testRestartAllInstancesWithZeroUnsetAndPositiveCounts() throws Exception {
    InMemoryNoTxKeyValueTable kvTable = new InMemoryNoTxKeyValueTable();
    DatasetId storeDatasetId =
        NamespaceId.SYSTEM.dataset(Constants.Service.SERVICE_INSTANCE_TABLE_NAME);

    DatasetFramework dsFramework = Mockito.mock(DatasetFramework.class);
    Mockito.when(dsFramework.hasInstance(storeDatasetId)).thenReturn(true);
    Mockito.when(dsFramework.getDataset(
            Mockito.eq(storeDatasetId), Mockito.anyMap(), Mockito.isNull()))
        .thenReturn(kvTable);

    DatasetServiceStore store = new DatasetServiceStore(dsFramework);
    store.startAndWait();
    try {
      // 1. Unset service instance count (null -> 0) produces an empty instances set.
      Assert.assertNull(store.getServiceInstance("unsetService"));
      store.setRestartAllInstancesRequest("unsetService", 100L, 200L, true);
      RestartServiceInstancesStatus unsetStatus =
          store.getLatestRestartInstancesRequest("unsetService");
      Assert.assertEquals("unsetService", unsetStatus.getServiceName());
      Assert.assertEquals(100L, unsetStatus.getStartTimeInMs());
      Assert.assertEquals(200L, unsetStatus.getEndTimeInMs());
      Assert.assertEquals(RestartStatus.SUCCESS, unsetStatus.getStatus());
      Assert.assertEquals(Collections.emptySet(), unsetStatus.getInstanceIds());

      // 2. Explicit instanceCount == 0 produces an empty instances set.
      store.setServiceInstance("zeroService", 0);
      Assert.assertEquals(Integer.valueOf(0), store.getServiceInstance("zeroService"));
      store.setRestartAllInstancesRequest("zeroService", 300L, 400L, false);
      RestartServiceInstancesStatus zeroStatus =
          store.getLatestRestartInstancesRequest("zeroService");
      Assert.assertEquals(RestartStatus.FAILURE, zeroStatus.getStatus());
      Assert.assertEquals(Collections.emptySet(), zeroStatus.getInstanceIds());

      // 3. Explicit instanceCount == 3 produces {0, 1, 2}.
      store.setServiceInstance("threeService", 3);
      Assert.assertEquals(Integer.valueOf(3), store.getServiceInstance("threeService"));
      store.setRestartAllInstancesRequest("threeService", 500L, 600L, true);
      RestartServiceInstancesStatus threeStatus =
          store.getLatestRestartInstancesRequest("threeService");
      Assert.assertEquals(RestartStatus.SUCCESS, threeStatus.getStatus());
      Assert.assertEquals(ImmutableSet.of(0, 1, 2), threeStatus.getInstanceIds());

      // 4. Single instance restart and missing latest request check.
      store.setRestartInstanceRequest("singleService", 10L, 20L, true, 2);
      Assert.assertEquals(
          ImmutableSet.of(2),
          store.getLatestRestartInstancesRequest("singleService").getInstanceIds());
      Assert.assertThrows(IllegalStateException.class,
          () -> store.getLatestRestartInstancesRequest("neverRestarted"));
    } finally {
      store.stopAndWait();
    }
    Assert.assertTrue(kvTable.closed.get());
  }

  private static final class InMemoryNoTxKeyValueTable implements NoTxKeyValueTable {

    private final Map<ByteBuffer, byte[]> entries = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void put(byte[] key, @Nullable byte[] value) {
      ByteBuffer keyBuf = ByteBuffer.wrap(key.clone());
      if (value == null) {
        entries.remove(keyBuf);
      } else {
        entries.put(keyBuf, value.clone());
      }
    }

    @Nullable
    @Override
    public byte[] get(byte[] key) {
      byte[] value = entries.get(ByteBuffer.wrap(key));
      return value == null ? null : value.clone();
    }

    @Override
    public void close() {
      closed.set(true);
    }
  }
}
