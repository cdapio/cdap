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

import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.metadata.MetadataRecordV2;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.DefaultLineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.LineageStoreReader;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.MessagingLineageWriter;
import co.cask.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import co.cask.cdap.data2.metadata.writer.MetadataOperation;
import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.data2.registry.BasicUsageRegistry;
import co.cask.cdap.data2.registry.MessagingUsageWriter;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.registry.UsageWriter;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
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
  private final WorkflowId workflow1 = NamespaceId.DEFAULT.app("app3").workflow("workflow1");

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
      lineageWriter.addAccess(run1, stream1, AccessType.UNKNOWN, flowlet1);
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

  @Test
  public void testWorkflow() throws InterruptedException, ExecutionException, TimeoutException {
    ProgramRunId workflowRunId = workflow1.run(RunIds.generate());

    BasicWorkflowToken token = new BasicWorkflowToken(1024);
    token.setCurrentNode("node1");
    token.put("key", "value");

    // Publish some workflow states
    WorkflowStateWriter workflowStateWriter = getInjector().getInstance(MessagingWorkflowStateWriter.class);
    workflowStateWriter.setWorkflowToken(workflowRunId, token);
    workflowStateWriter.addWorkflowNodeState(workflowRunId, new WorkflowNodeStateDetail("action1", NodeStatus.RUNNING));

    // Try to read, should have nothing
    Store store = getInjector().getInstance(DefaultStore.class);
    WorkflowToken workflowToken = store.getWorkflowToken(workflow1, workflowRunId.getRun());
    Assert.assertNull(workflowToken.get("key"));

    // Start the MetadataSubscriberService
    MetadataSubscriberService subscriberService = getInjector().getInstance(MetadataSubscriberService.class);
    subscriberService.startAndWait();
    try {
      // Verify the WorkflowToken
      Tasks.waitFor("value", () ->
        Optional.ofNullable(
          store.getWorkflowToken(workflow1, workflowRunId.getRun()).get("key")
        ).map(Value::toString).orElse(null)
      , 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Verify the workflow node state
      Tasks.waitFor(NodeStatus.RUNNING, () ->
        store.getWorkflowNodeStates(workflowRunId).stream().findFirst()
             .map(WorkflowNodeStateDetail::getNodeStatus).orElse(null),
        10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Update the node state
      workflowStateWriter.addWorkflowNodeState(workflowRunId,
                                               new WorkflowNodeStateDetail("action1", NodeStatus.COMPLETED));
      // Verify the updated node state
      Tasks.waitFor(NodeStatus.COMPLETED, () ->
        store.getWorkflowNodeStates(workflowRunId).stream().findFirst()
             .map(WorkflowNodeStateDetail::getNodeStatus).orElse(null),
        10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      subscriberService.stopAndWait();
    }
  }

  @Test
  public void testMetadata() throws InterruptedException, TimeoutException, ExecutionException {
    ProgramRunId workflowRunId = workflow1.run(RunIds.generate());
    MetadataEntity entity = MetadataEntity.ofDataset("myns", "myds");

    // Try to read, should have nothing
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    MetadataRecordV2 meta = metadataStore.getMetadata(MetadataScope.USER, entity);
    Assert.assertTrue(meta.getProperties().isEmpty());
    Assert.assertTrue(meta.getTags().isEmpty());

    // Start the MetadataSubscriberService
    MetadataPublisher metadataPublisher = getInjector().getInstance(MessagingMetadataPublisher.class);
    MetadataSubscriberService subscriberService = getInjector().getInstance(MetadataSubscriberService.class);
    subscriberService.startAndWait();
    try {

      // publish metadata put
      Map<String, String> propertiesToAdd = ImmutableMap.of("a", "x", "b", "z");
      Set<String> tagsToAdd = ImmutableSet.of("t1", "t2");
      metadataPublisher.publish(workflowRunId, new MetadataOperation(entity, MetadataOperation.Type.PUT,
                                                                     new Metadata(propertiesToAdd, tagsToAdd)));

      // wait until meta data is written
      waitForMetadata(entity, metadataStore, 2, 2);

      // validate correctness of meta data written
      meta = metadataStore.getMetadata(MetadataScope.USER, entity);
      Assert.assertEquals(propertiesToAdd, meta.getProperties());
      Assert.assertEquals(tagsToAdd, meta.getTags());

      // publish metadata delete
      metadataPublisher.publish(workflowRunId, new MetadataOperation(entity, MetadataOperation.Type.DELETE,
                                                                     new Metadata(ImmutableMap.of("a", ""),
                                                                                  ImmutableSet.of("t1", "t3"))));

      // wait until meta data is written
      waitForMetadata(entity, metadataStore, 1, 1);

      // validate correctness of meta data after delete
      meta = metadataStore.getMetadata(MetadataScope.USER, entity);
      Assert.assertEquals(ImmutableMap.of("b", "z"), meta.getProperties());
      Assert.assertEquals(ImmutableSet.of("t2"), meta.getTags());

      // publish metadata put properties
      metadataPublisher.publish(workflowRunId,
                                new MetadataOperation(entity, MetadataOperation.Type.PUT,
                                                      new Metadata(propertiesToAdd, Collections.emptySet())));

      // wait until meta data is written
      // one of the property key already exist so for that value will be just overwritten hence size is 2
      waitForMetadata(entity, metadataStore, 2, 1);

      // publish metadata put tags
      metadataPublisher.publish(workflowRunId,
                                new MetadataOperation(entity, MetadataOperation.Type.PUT,
                                                      new Metadata(Collections.emptyMap(), tagsToAdd)));

      // wait until meta data is written
      // one of the tags already exists hence size is 2
      waitForMetadata(entity, metadataStore, 2, 2);

      // publish delete all properties
      metadataPublisher.publish(workflowRunId,
                                new MetadataOperation(entity, MetadataOperation.Type.DELETE_ALL_PROPERTIES, null));

      // wait until meta data is written
      waitForMetadata(entity, metadataStore, 0, 2);

      // publish delete all tags
      metadataPublisher.publish(workflowRunId,
                                new MetadataOperation(entity, MetadataOperation.Type.DELETE_ALL_TAGS, null));

      waitForMetadata(entity, metadataStore, 0, 0);

      // publish metadata put tags
      metadataPublisher.publish(workflowRunId,
                                new MetadataOperation(entity, MetadataOperation.Type.PUT,
                                                      new Metadata(propertiesToAdd, tagsToAdd)));

      // wait until meta data is written
      waitForMetadata(entity, metadataStore, 2, 2);

      // publish delete all
      metadataPublisher.publish(workflowRunId,
                                new MetadataOperation(entity, MetadataOperation.Type.DELETE_ALL, null));

      // wait until meta data is written
      waitForMetadata(entity, metadataStore, 0, 0);

    } finally {
      subscriberService.stopAndWait();
    }
  }

  private void waitForMetadata(MetadataEntity entity, MetadataStore metadataStore, int propertiesSize,
                               int tagSize) throws TimeoutException, InterruptedException, ExecutionException {
    Tasks.waitFor(true, () -> {
      MetadataRecordV2 record = metadataStore.getMetadata(MetadataScope.USER, entity);
      return record.getProperties().size() == propertiesSize && record.getTags().size() == tagSize;
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private DatasetFramework getDatasetFramework() {
    return getInjector().getInstance(DatasetFramework.class);
  }
}
