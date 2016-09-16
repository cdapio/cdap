/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.store.remote;

import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests implementation of {@link RemoteRuntimeUsageRegistry}, by using it to perform writes/updates, and then using a
 * local {@link UsageRegistry} to verify the updates/writes.
 */
public class RemoteRuntimeUsageRegistryTest extends AppFabricTestBase {

  private static UsageRegistry usageRegistry;
  private static RemoteRuntimeUsageRegistry runtimeUsageRegistry;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = getInjector();
    usageRegistry = injector.getInstance(UsageRegistry.class);
    runtimeUsageRegistry = injector.getInstance(RemoteRuntimeUsageRegistry.class);
  }

  @Test
  public void testSimpleCase() {
    ApplicationId appId = NamespaceId.DEFAULT.app("test_app");
    ProgramId flowId1 = appId.flow("test_flow1");
    ProgramId flowId2 = appId.flow("test_flow2");

    // should be no data initially
    Assert.assertEquals(0, usageRegistry.getDatasets(appId).size());
    Assert.assertEquals(0, usageRegistry.getStreams(appId).size());

    Assert.assertEquals(0, usageRegistry.getDatasets(flowId1).size());
    Assert.assertEquals(0, usageRegistry.getStreams(flowId1).size());

    Assert.assertEquals(0, usageRegistry.getDatasets(flowId2).size());
    Assert.assertEquals(0, usageRegistry.getStreams(flowId2).size());

    DatasetId datasetId1 = NamespaceId.DEFAULT.dataset("test_dataset1");
    runtimeUsageRegistry.register(flowId1, datasetId1);

    ImmutableSet<DatasetId> datasetsUsedByFlow1 = ImmutableSet.of(datasetId1);
    Assert.assertEquals(datasetsUsedByFlow1, usageRegistry.getDatasets(appId));
    Assert.assertEquals(0, usageRegistry.getStreams(appId).size());

    Assert.assertEquals(datasetsUsedByFlow1, usageRegistry.getDatasets(flowId1));
    Assert.assertEquals(0, usageRegistry.getStreams(flowId1).size());

    Assert.assertEquals(0, usageRegistry.getDatasets(flowId2).size());
    Assert.assertEquals(0, usageRegistry.getStreams(flowId2).size());

    DatasetId datasetId2 = NamespaceId.DEFAULT.dataset("test_dataset2");
    StreamId streamId = NamespaceId.DEFAULT.stream("test_stream");
    runtimeUsageRegistry.register(flowId2, datasetId1);
    runtimeUsageRegistry.register(flowId2, datasetId2);
    runtimeUsageRegistry.register(flowId2, streamId);

    ImmutableSet<DatasetId> datasetsUsedByFlow2 = ImmutableSet.of(datasetId1, datasetId2);
    ImmutableSet<StreamId> streamsUsedByFlow2 = ImmutableSet.of(streamId);
    Assert.assertEquals(Sets.union(datasetsUsedByFlow1, datasetsUsedByFlow2), usageRegistry.getDatasets(appId));
    Assert.assertEquals(streamsUsedByFlow2, usageRegistry.getStreams(appId));

    Assert.assertEquals(datasetsUsedByFlow1, usageRegistry.getDatasets(flowId1));
    Assert.assertEquals(0, usageRegistry.getStreams(flowId1).size());

    Assert.assertEquals(datasetsUsedByFlow2, usageRegistry.getDatasets(flowId2));
    Assert.assertEquals(streamsUsedByFlow2, usageRegistry.getStreams(flowId2));
  }
}
