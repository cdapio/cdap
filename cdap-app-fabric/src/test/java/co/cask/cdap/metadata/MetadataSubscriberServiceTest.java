/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.LineageStoreReader;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.MessagingLineageWriter;
import co.cask.cdap.data2.registry.BasicUsageRegistry;
import co.cask.cdap.data2.registry.MessagingUsageWriter;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.registry.UsageWriter;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit test for {@link MetadataSubscriberService} and corresponding writers.
 */
public class MetadataSubscriberServiceTest extends AppFabricTestBase {

  private final StreamId stream1 = NamespaceId.DEFAULT.stream("stream1");
  private final DatasetId dataset1 = NamespaceId.DEFAULT.dataset("dataset1");
  private final DatasetId dataset2 = NamespaceId.DEFAULT.dataset("dataset2");
  private final DatasetId dataset3 = NamespaceId.DEFAULT.dataset("dataset3");

  private final ProgramId flow1 = NamespaceId.DEFAULT.app("app1").program(ProgramType.FLOW, "flow1");
  private final FlowletId flowlet1 = flow1.flowlet("flowlet1");

  private final ProgramId spark1 = NamespaceId.DEFAULT.app("app2").program(ProgramType.SPARK, "spark1");

  @Test
  public void testSubscriber() throws InterruptedException, ExecutionException, TimeoutException {
    DatasetId lineageDatasetId = NamespaceId.DEFAULT.dataset("testSubscriberLineage");
    DatasetId usageDatasetId = NamespaceId.DEFAULT.dataset("testSubscriberUsage");

    // Write out some lineage information
    LineageWriter lineageWriter = getInjector().getInstance(MessagingLineageWriter.class);
    ProgramRunId run1 = flow1.run(RunIds.generate());
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    lineageWriter.addAccess(run1, dataset2, AccessType.WRITE);

    LineageStoreReader lineageReader = new DefaultLineageStoreReader(getDatasetFramework(),
                                                                     getTxClient(), lineageDatasetId);

    // Try to read lineage, which should be empty since we haven't start the MetadataSubscriberService yet.
    Set<NamespacedEntityId> entities = lineageReader.getEntitiesForRun(run1);
    Assert.assertTrue(entities.isEmpty());

    // Emit some usages
    UsageWriter usageWriter = getInjector().getInstance(MessagingUsageWriter.class);
    usageWriter.register(spark1, dataset1);
    usageWriter.registerAll(Collections.singleton(spark1), dataset3);

    // Try to read usage, should have nothing
    UsageRegistry usageRegistry = new BasicUsageRegistry(getDatasetFramework(), getTxClient(), usageDatasetId);
    Assert.assertTrue(usageRegistry.getDatasets(spark1).isEmpty());


    // Start the MetadataSubscriberService
    MetadataSubscriberService subscriberService = getInjector().getInstance(MetadataSubscriberService.class);
    subscriberService
      .setLineageDatasetId(lineageDatasetId)
      .setUsageDatasetId(usageDatasetId)
      .startAndWait();

    try {
      // Verifies lineage has been written
      Set<NamespacedEntityId> expectedLineage = new HashSet<>(Arrays.asList(run1.getParent(), dataset1, dataset2));
      Tasks.waitFor(true, () -> expectedLineage.equals(lineageReader.getEntitiesForRun(run1)),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Emit one more lineage
      lineageWriter.addAccess(run1, stream1, AccessType.UNKNOWN);
      expectedLineage.add(stream1);
      Tasks.waitFor(true, () -> expectedLineage.equals(lineageReader.getEntitiesForRun(run1)),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // There shouldn't be any lineage for the "spark1" program, as only usage has been emitted.
      Assert.assertTrue(lineageReader.getRelations(spark1, 0L, Long.MAX_VALUE, x -> true).isEmpty());

      // Verifies usage has been written
      Set<EntityId> expectedUsage = new HashSet<>(Arrays.asList(dataset1, dataset3));
      Tasks.waitFor(true, () -> expectedUsage.equals(usageRegistry.getDatasets(spark1)),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Emit one more usage
      usageWriter.register(flow1, stream1);
      expectedUsage.clear();
      expectedUsage.add(stream1);
      Tasks.waitFor(true, () -> expectedUsage.equals(usageRegistry.getStreams(flow1)),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    } finally {
      subscriberService.stopAndWait();
    }

  }

  private DatasetFramework getDatasetFramework() {
    return getInjector().getInstance(DatasetFramework.class);
  }
}
