/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.api.workflow.Value;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategyType;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.LineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.field.EndPointField;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageReader;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriter;
import io.cdap.cdap.data2.metadata.writer.MessagingLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import io.cdap.cdap.data2.metadata.writer.MetadataOperation;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.registry.MessagingUsageWriter;
import io.cdap.cdap.data2.registry.UsageRegistry;
import io.cdap.cdap.data2.registry.UsageWriter;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.profile.AdminEventPublisher;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.RollbackDetail;
import io.cdap.cdap.messaging.StoreRequest;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.messaging.service.CoreMessagingService;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.messaging.store.leveldb.LevelDBTableFactory;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.proto.metadata.lineage.ProgramRunOperations;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.scheduler.ProgramScheduleService;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.Read;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Unit test for {@link MetadataSubscriberService} and corresponding writers.
 */
public class MetadataSubscriberServiceTest extends AppFabricTestBase {

  private final DatasetId dataset1 = NamespaceId.DEFAULT.dataset("dataset1");
  private final DatasetId dataset2 = NamespaceId.DEFAULT.dataset("dataset2");
  private final DatasetId dataset3 = NamespaceId.DEFAULT.dataset("dataset3");

  private final ProgramId service1 = NamespaceId.DEFAULT.app("app1").program(ProgramType.SERVICE, "service1");

  private final ProgramId spark1 = NamespaceId.DEFAULT.app("app2").program(ProgramType.SPARK, "spark1");
  private final WorkflowId workflow1 = NamespaceId.DEFAULT.app("app3").workflow("workflow1");

  @BeforeClass
  public static void beforeClass() throws Throwable {
    CConfiguration cConfiguration = createBasicCConf();
    // use a fast retry strategy with not too many retries, to speed up the test
    String prefix = "system.metadata.";
    cConfiguration.set(prefix + Constants.Retry.TYPE, RetryStrategyType.FIXED_DELAY.toString());
    cConfiguration.set(prefix + Constants.Retry.MAX_RETRIES, "100");
    cConfiguration.set(prefix + Constants.Retry.MAX_TIME_SECS, "10");
    cConfiguration.set(prefix + Constants.Retry.DELAY_BASE_MS, "200");
    cConfiguration.set(Constants.Metadata.MESSAGING_RETRIES_ON_CONFLICT, "20");
    // use a messaging service that helps reproduce race conditions in metadata consumption
    initializeAndStartServices(cConfiguration, new PrivateModule() {
      @Override
      protected void configure() {
        bind(TableFactory.class).to(LevelDBTableFactory.class).in(Scopes.SINGLETON);
        bind(MessagingService.class).to(DelayMessagingService.class).in(Scopes.SINGLETON);
        expose(MessagingService.class);
      }
    });
  }

  @Test
  public void testSubscriber() throws InterruptedException, ExecutionException, TimeoutException {

    LineageStoreReader lineageReader = getInjector().getInstance(LineageStoreReader.class);
    ProgramRunId run1 = service1.run(RunIds.generate());

    // Try to read lineage, which should be empty since we haven't start the MetadataSubscriberService yet.
    Set<NamespacedEntityId> entities = lineageReader.getEntitiesForRun(run1);
    Assert.assertTrue(entities.isEmpty());

    // Write out some lineage information
    LineageWriter lineageWriter = getInjector().getInstance(MessagingLineageWriter.class);
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    lineageWriter.addAccess(run1, dataset2, AccessType.WRITE);

    // Write the field level lineage
    FieldLineageWriter fieldLineageWriter = getInjector().getInstance(MessagingLineageWriter.class);
    ProgramRunId spark1Run1 = spark1.run(RunIds.generate(100));
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("ns", "endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "name", "address");
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint2"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(read);
    operations.add(write);
    operations.add(parse);
    FieldLineageInfo info1 = new FieldLineageInfo(operations);
    fieldLineageWriter.write(spark1Run1, info1);

    ProgramRunId spark1Run2 = spark1.run(RunIds.generate(200));
    fieldLineageWriter.write(spark1Run2, info1);

    List<Operation> operations2 = new ArrayList<>();
    operations2.add(read);
    operations2.add(parse);
    TransformOperation normalize = new TransformOperation("normalize", "normalize address",
                                                          Collections.singletonList(InputField.of("parse", "address")),
                                                          "address");
    operations2.add(normalize);
    WriteOperation anotherWrite = new WriteOperation("anotherwrite", "write data", EndPoint.of("ns", "endpoint2"),
                                                     Arrays.asList(InputField.of("read", "offset"),
                                                                   InputField.of("parse", "name"),
                                                                   InputField.of("normalize", "address")));
    operations2.add(anotherWrite);
    FieldLineageInfo info2 = new FieldLineageInfo(operations2);
    ProgramRunId spark1Run3 = spark1.run(RunIds.generate(300));
    fieldLineageWriter.write(spark1Run3, info2);

    // Emit some usages
    UsageWriter usageWriter = getInjector().getInstance(MessagingUsageWriter.class);
    usageWriter.register(spark1, dataset1);
    usageWriter.registerAll(Collections.singleton(spark1), dataset3);

    // Verifies lineage has been written
    Set<NamespacedEntityId> expectedLineage = new HashSet<>(Arrays.asList(run1.getParent(), dataset1, dataset2));
    Tasks.waitFor(true, () -> expectedLineage.equals(lineageReader.getEntitiesForRun(run1)),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // There shouldn't be any lineage for the "spark1" program, as only usage has been emitted.
    Assert.assertTrue(lineageReader.getRelations(spark1, 0L, Long.MAX_VALUE, x -> true).isEmpty());

    FieldLineageReader fieldLineageReader = getInjector().getInstance(FieldLineageReader.class);

    Set<Operation> expectedOperations = new HashSet<>();
    expectedOperations.add(read);
    expectedOperations.add(anotherWrite);
    List<ProgramRunOperations> expected = new ArrayList<>();
    // Descending order of program execution
    expected.add(new ProgramRunOperations(Collections.singleton(spark1Run3), expectedOperations));

    expectedOperations = new HashSet<>();
    expectedOperations.add(read);
    expectedOperations.add(write);
    expected.add(new ProgramRunOperations(new HashSet<>(Arrays.asList(spark1Run1, spark1Run2)),
                                          expectedOperations));

    EndPointField endPointField = new EndPointField(EndPoint.of("ns", "endpoint2"), "offset");
    Tasks.waitFor(expected, () -> fieldLineageReader.getIncomingOperations(endPointField, 1L, Long.MAX_VALUE - 1),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Verifies usage has been written
    Set<EntityId> expectedUsage = new HashSet<>(Arrays.asList(dataset1, dataset3));
    UsageRegistry usageRegistry = getInjector().getInstance(UsageRegistry.class);
    Tasks.waitFor(true, () -> expectedUsage.equals(usageRegistry.getDatasets(spark1)),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testWorkflow() throws InterruptedException, ExecutionException, TimeoutException {
    ProgramRunId workflowRunId = workflow1.run(RunIds.generate());

    // Try to read, should have nothing
    Store store = getInjector().getInstance(DefaultStore.class);
    WorkflowToken workflowToken = store.getWorkflowToken(workflow1, workflowRunId.getRun());
    Assert.assertNull(workflowToken.get("key"));

    BasicWorkflowToken token = new BasicWorkflowToken(1024);
    token.setCurrentNode("node1");
    token.put("key", "value");

    // Publish some workflow states
    WorkflowStateWriter workflowStateWriter = getInjector().getInstance(MessagingWorkflowStateWriter.class);
    workflowStateWriter.setWorkflowToken(workflowRunId, token);
    workflowStateWriter.addWorkflowNodeState(workflowRunId, new WorkflowNodeStateDetail("action1", NodeStatus.RUNNING));

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
  }

  @Test
  public void testMetadata() throws InterruptedException, TimeoutException, ExecutionException, IOException {
    ProgramRunId workflowRunId = workflow1.run(RunIds.generate());
    MetadataEntity entity = MetadataEntity.ofDataset("myns", "myds");

    // Try to read, should have nothing
    MetadataStorage metadataStorage = getInjector().getInstance(MetadataStorage.class);
    Metadata meta = metadataStorage.read(new Read(entity, MetadataScope.USER));
    Assert.assertTrue(meta.getProperties().isEmpty());
    Assert.assertTrue(meta.getTags().isEmpty());

    MetadataPublisher metadataPublisher = getInjector().getInstance(MessagingMetadataPublisher.class);
    final String descriptionKey = MetadataConstants.DESCRIPTION_KEY;
    final String creationTimeKey = MetadataConstants.CREATION_TIME_KEY;

    // publish a create event
    Map<String, String> props = ImmutableMap.of("x", "y", descriptionKey, "desc1", creationTimeKey, "123456");
    Set<String> tags = ImmutableSet.of("sometag");
    metadataPublisher.publish(NamespaceId.SYSTEM, new MetadataOperation.Create(entity, props, tags));

    // wait until meta data is written
    waitForSystemMetadata(entity, metadataStorage, 3, 1);

    // validate correctness of meta data after create
    meta = metadataStorage.read(new Read(entity, MetadataScope.SYSTEM));
    Assert.assertEquals(props, meta.getProperties(MetadataScope.SYSTEM));
    Assert.assertEquals(tags, meta.getTags(MetadataScope.SYSTEM));

    // publish another create event with different create time, no description, different tags
    Set<String> tags2 = ImmutableSet.of("another", "two");
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Create(
      entity, ImmutableMap.of(creationTimeKey, "9876543", "new", "prop"), tags2));
    // wait until meta data is written
    waitForSystemMetadata(entity, metadataStorage, 3, 2);

    // validate correctness of meta data: creation time and description unchanged, other new property there
    meta = metadataStorage.read(new Read(entity, MetadataScope.SYSTEM));
    Assert.assertEquals(ImmutableMap.of(creationTimeKey, "123456", descriptionKey, "desc1", "new", "prop"),
                        meta.getProperties(MetadataScope.SYSTEM));
    Assert.assertEquals(tags2, meta.getTags(MetadataScope.SYSTEM));

    // publish another create event without create time, different description, no tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Create(
      entity, ImmutableMap.of(descriptionKey, "some"), Collections.emptySet()));
    // wait until meta data is written
    waitForSystemMetadata(entity, metadataStorage, 2, 0);

    // validate correctness of meta data: same creation time, updated description and other props and tags
    meta = metadataStorage.read(new Read(entity, MetadataScope.SYSTEM));
    Assert.assertEquals(ImmutableMap.of(creationTimeKey, "123456", descriptionKey, "some"),
                        meta.getProperties(MetadataScope.SYSTEM));
    Assert.assertEquals(Collections.emptySet(), meta.getTags(MetadataScope.SYSTEM));

    // publish metadata put
    Map<String, String> propertiesToAdd = ImmutableMap.of("a", "x", "b", "z");
    Set<String> tagsToAdd = ImmutableSet.of("t1", "t2");
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Put(entity, propertiesToAdd, tagsToAdd));

    // wait until meta data is written
    waitForMetadata(entity, metadataStorage, 2, 2);

    // validate correctness of meta data written
    meta = metadataStorage.read(new Read(entity, MetadataScope.USER));
    Assert.assertEquals(propertiesToAdd, meta.getProperties(MetadataScope.USER));
    Assert.assertEquals(tagsToAdd, meta.getTags(MetadataScope.USER));

    // publish metadata delete
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Delete(
      entity, Collections.singleton("a"), ImmutableSet.of("t1", "t3")));

    // wait until meta data is written
    waitForMetadata(entity, metadataStorage, 1, 1);

    // validate correctness of meta data after delete
    meta = metadataStorage.read(new Read(entity, MetadataScope.USER));
    Assert.assertEquals(ImmutableMap.of("b", "z"), meta.getProperties(MetadataScope.USER));
    Assert.assertEquals(ImmutableSet.of("t2"), meta.getTags(MetadataScope.USER));

    // publish metadata put properties
    metadataPublisher.publish(workflowRunId,
                              new MetadataOperation.Put(entity, propertiesToAdd, Collections.emptySet()));

    // wait until meta data is written
    // one of the property key already exist so for that value will be just overwritten hence size is 2
    waitForMetadata(entity, metadataStorage, 2, 1);

    // publish metadata put tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Put(entity, Collections.emptyMap(), tagsToAdd));

    // wait until meta data is written
    // one of the tags already exists hence size is 2
    waitForMetadata(entity, metadataStorage, 2, 2);

    // publish delete all properties
    metadataPublisher.publish(workflowRunId, new MetadataOperation.DeleteAllProperties(entity));

    // wait until meta data is written
    waitForMetadata(entity, metadataStorage, 0, 2);

    // publish delete all tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.DeleteAllTags(entity));

    // wait until meta data is written
    waitForMetadata(entity, metadataStorage, 0, 0);

    // publish metadata put tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Put(entity, propertiesToAdd, tagsToAdd));

    // wait until meta data is written
    waitForMetadata(entity, metadataStorage, 2, 2);

    // publish delete all
    metadataPublisher.publish(workflowRunId, new MetadataOperation.DeleteAll(entity));

    // wait until meta data is written
    waitForMetadata(entity, metadataStorage, 0, 0);

    // publish metadata put tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Put(entity, propertiesToAdd, tagsToAdd));

    // wait until meta data is written
    waitForMetadata(entity, metadataStorage, 2, 2);

    // publish drop entity
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Drop(entity));
    // wait until meta data is deleted
    waitForSystemMetadata(entity, metadataStorage, 0, 0);
    waitForMetadata(entity, metadataStorage, 0, 0);
  }

  @Test
  public void testProfileMetadata() throws Exception {
    Injector injector = getInjector();

    ApplicationSpecification appSpec = Specifications.from(new AppWithWorkflow());
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    ProgramId workflowId = appId.workflow("SampleWorkflow");
    ScheduleId scheduleId = appId.schedule("tsched1");

    // publish a creation of a schedule that will never exist
    // this tests that such a message is eventually discarded
    // note that for this test, we configure a fast retry strategy and a small number of retries
    // therefore this will cost only a few seconds delay
    publishBogusCreationEvent();

    // get the mds should be empty property since we haven't started the MetadataSubscriberService
    MetadataStorage mds = injector.getInstance(MetadataStorage.class);
    Assert.assertEquals(Collections.emptyMap(), mds.read(new Read(workflowId.toMetadataEntity())).getProperties());
    Assert.assertEquals(Collections.emptyMap(), mds.read(new Read(scheduleId.toMetadataEntity())).getProperties());

    // add a app with workflow to app meta store
    // note: since we bypass the app-fabric when adding this app, no ENTITY_CREATION message
    // will be published for the app (it happens in app lifecycle service). Therefore this
    // app must exist before assigning the profile for the namespace, otherwise the app's
    // programs will not receive the profile metadata.
    Store store = injector.getInstance(DefaultStore.class);
    store.addApplication(appId, appSpec);

    // set default namespace to use the profile, since now MetadataSubscriberService is not started,
    // it should not affect the mds
    PreferencesService preferencesService = injector.getInstance(PreferencesService.class);
    preferencesService.setProperties(NamespaceId.DEFAULT,
                                     Collections.singletonMap(SystemArguments.PROFILE_NAME,
                                                              ProfileId.NATIVE.getScopedName()));

    // add a schedule to schedule store
    ProgramScheduleService scheduleService = injector.getInstance(ProgramScheduleService.class);
    scheduleService.add(new ProgramSchedule("tsched1", "one time schedule", workflowId,
                                            Collections.emptyMap(),
                                            new TimeTrigger("* * ? * 1"), ImmutableList.of()));

    // add a new profile in default namespace
    ProfileService profileService = injector.getInstance(ProfileService.class);
    ProfileId myProfile = new ProfileId(NamespaceId.DEFAULT.getNamespace(), "MyProfile");
    Profile profile1 = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                   Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    profileService.saveProfile(myProfile, profile1);

    // add a second profile in default namespace
    ProfileId myProfile2 = new ProfileId(NamespaceId.DEFAULT.getNamespace(), "MyProfile2");
    Profile profile2 = new Profile("MyProfile2", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                   Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    profileService.saveProfile(myProfile2, profile2);

    try {
      // Verify the workflow profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(),
                    () -> getProfileProperty(mds, workflowId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(),
                    () -> getProfileProperty(mds, scheduleId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);


      // set default namespace to use my profile
      preferencesService.setProperties(NamespaceId.DEFAULT,
                                       Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:MyProfile"));

      // Verify the workflow profile metadata is updated to my profile
      Tasks.waitFor(myProfile.getScopedName(), () -> getProfileProperty(mds, workflowId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to my profile
      Tasks.waitFor(myProfile.getScopedName(), () -> getProfileProperty(mds, scheduleId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // set app level to use my profile 2
      preferencesService.setProperties(appId,
                                       Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:MyProfile2"));

      // set instance level to system profile
      preferencesService.setProperties(Collections.singletonMap(SystemArguments.PROFILE_NAME,
                                                                ProfileId.NATIVE.getScopedName()));

      // Verify the workflow profile metadata is updated to MyProfile2 which is at app level
      Tasks.waitFor(myProfile2.getScopedName(), () -> getProfileProperty(mds, workflowId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to MyProfile2 which is at app level
      Tasks.waitFor(myProfile2.getScopedName(), () -> getProfileProperty(mds, scheduleId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // remove the preferences at instance level, should not affect the metadata
      preferencesService.deleteProperties();

      // Verify the workflow profile metadata is updated to default profile
      Tasks.waitFor(myProfile2.getScopedName(), () -> getProfileProperty(mds, workflowId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to default profile
      Tasks.waitFor(myProfile2.getScopedName(), () -> getProfileProperty(mds, scheduleId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // remove app level pref should let the programs/schedules use ns level pref
      preferencesService.deleteProperties(appId);

      // Verify the workflow profile metadata is updated to MyProfile
      Tasks.waitFor(myProfile.getScopedName(), () -> getProfileProperty(mds, workflowId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to MyProfile
      Tasks.waitFor(myProfile.getScopedName(), () -> getProfileProperty(mds, scheduleId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // remove ns level pref so no pref is there
      preferencesService.deleteProperties(NamespaceId.DEFAULT);

      // Verify the workflow profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(),
                    () -> getProfileProperty(mds, workflowId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(),
                    () -> getProfileProperty(mds, scheduleId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      // stop and clean up the store
      preferencesService.deleteProperties(NamespaceId.DEFAULT);
      preferencesService.deleteProperties();
      preferencesService.deleteProperties(appId);
      store.removeAll(NamespaceId.DEFAULT);
      scheduleService.delete(scheduleId);
      profileService.disableProfile(myProfile);
      profileService.disableProfile(myProfile2);
      profileService.deleteAllProfiles(myProfile.getNamespaceId());
      mds.apply(new MetadataMutation.Drop(workflowId.toMetadataEntity()), MutationOptions.DEFAULT);
      mds.apply(new MetadataMutation.Drop(scheduleId.toMetadataEntity()), MutationOptions.DEFAULT);
    }
  }

  private void publishBogusCreationEvent() {
    MultiThreadMessagingContext messagingContext =
      new MultiThreadMessagingContext(getInjector().getInstance(MessagingService.class));
    AdminEventPublisher adminEventPublisher =
      new AdminEventPublisher(getInjector().getInstance(CConfiguration.class), messagingContext);
    adminEventPublisher.publishScheduleCreation(NamespaceId.DEFAULT.app("nosuch").schedule("none"),
                                                System.currentTimeMillis());
  }

  @Test
  public void testProfileMetadataWithNoProfilePreferences() throws Exception {
    Injector injector = getInjector();

    // add a new profile in default namespace
    ProfileService profileService = injector.getInstance(ProfileService.class);
    ProfileId myProfile = new ProfileId(NamespaceId.DEFAULT.getNamespace(), "MyProfile");
    Profile profile1 = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                   Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    profileService.saveProfile(myProfile, profile1);

    // add a app with workflow to app meta store
    ApplicationSpecification appSpec = Specifications.from(new AppWithWorkflow());
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    ProgramId workflowId = appId.workflow("SampleWorkflow");

    // get the metadata - should be empty since we haven't deployed the app
    MetadataStorage mds = injector.getInstance(MetadataStorage.class);
    Assert.assertEquals(Collections.emptyMap(), mds.read(new Read(workflowId.toMetadataEntity())).getProperties());

    Store store = injector.getInstance(DefaultStore.class);
    store.addApplication(appId, appSpec);

    // set default namespace to use the profile, since now MetadataSubscriberService is not started,
    // it should not affect the mds
    PreferencesService preferencesService = injector.getInstance(PreferencesService.class);
    preferencesService.setProperties(NamespaceId.DEFAULT,
                                     Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:MyProfile"));

    try {
      // Verify the workflow profile metadata is updated to my profile
      Tasks.waitFor(myProfile.getScopedName(), () -> getProfileProperty(mds, workflowId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Set the property without profile is a replacement of the preference, so it is same as deletion of the profile
      preferencesService.setProperties(NamespaceId.DEFAULT, Collections.emptyMap());

      // Verify the workflow profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(), () -> getProfileProperty(mds, workflowId),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      // stop and clean up the store
      preferencesService.deleteProperties(NamespaceId.DEFAULT);
      store.removeAll(NamespaceId.DEFAULT);
      profileService.disableProfile(myProfile);
      profileService.deleteProfile(myProfile);
      mds.apply(new MetadataMutation.Drop(workflowId.toMetadataEntity()), MutationOptions.DEFAULT);
    }
  }

  @Nullable
  private String getProfileProperty(MetadataStorage mds, NamespacedEntityId workflowId) throws IOException {
    return mds.read(new Read(workflowId.toMetadataEntity()))
      .getProperties(MetadataScope.SYSTEM).get("profile");
  }

  private void waitForMetadata(MetadataEntity entity, MetadataStorage metadataStorage, int propertiesSize,
                               int tagSize) throws TimeoutException, InterruptedException, ExecutionException {
    Tasks.waitFor(true, () -> {
      Metadata meta = metadataStorage.read(new Read(entity, MetadataScope.USER));
      return meta.getProperties().size() == propertiesSize && meta.getTags().size() == tagSize;
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private void waitForSystemMetadata(MetadataEntity entity, MetadataStorage metadataStorage, int propertiesSize,
                                     int tagSize) throws TimeoutException, InterruptedException, ExecutionException {
    Tasks.waitFor(true, () -> {
      Metadata meta = metadataStorage.read(new Read(entity, MetadataScope.SYSTEM));
      return meta.getProperties().size() == propertiesSize && meta.getTags().size() == tagSize;
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testAppDeletionMessage() throws Exception {
    Injector injector = getInjector();

    // get the alert publisher
    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    MessagingService messagingService = injector.getInstance(MessagingService.class);
    MultiThreadMessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
    AdminEventPublisher publisher = new AdminEventPublisher(cConf, messagingContext);

    // get the mds and put some workflow metadata in that, the publish of app deletion message should get the metadata
    // deleted
    MetadataStorage mds = injector.getInstance(MetadataStorage.class);

    // use an app with all program types to get all specification tested
    ApplicationId appId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    ProgramId workflowId = appId.workflow(AllProgramsApp.NoOpWorkflow.NAME);
    // generate an app spec from the application
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    // need to put metadata on workflow since we currently only set or delete workflow metadata
    mds.apply(new MetadataMutation.Update(workflowId.toMetadataEntity(),
                                          new Metadata(MetadataScope.SYSTEM,
                                                       Collections.singletonMap("profile",
                                                                                ProfileId.NATIVE.getScopedName()))),
              MutationOptions.DEFAULT);
    Assert.assertEquals(ProfileId.NATIVE.getScopedName(), getProfileProperty(mds, workflowId));

    // publish app deletion message
    publisher.publishAppDeletion(appId, appSpec);

    // Verify the workflow profile metadata is removed because of the publish app deletion message
    Tasks.waitFor(true, () -> mds.read(new Read(workflowId.toMetadataEntity())).isEmpty(),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  /**
   * A messaging service that inserts a delay after publishing each message. This slows
   * down the test a little (from 7  to 11 seconds), but helps reproduce race conditions
   * that can happen when a message is processed by the subscriber before the metadata
   * changes that caused the message have been committed.
   */
  public static class DelayMessagingService extends CoreMessagingService {

    private static final Logger LOG = LoggerFactory.getLogger(DelayMessagingService.class);

    @Inject
    DelayMessagingService(CConfiguration cConf, TableFactory tableFactory,
                          MetricsCollectionService metricsCollectionService) {
      super(cConf, tableFactory, metricsCollectionService);
    }

    @Nullable
    @Override
    public RollbackDetail publish(StoreRequest request) throws TopicNotFoundException, IOException {
      RollbackDetail result = super.publish(request);
      try {
        LOG.debug("Sleeping 200ms after message publish to topic '{}'", request.getTopicId());
        TimeUnit.MILLISECONDS.sleep(200L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return result;
    }
  }
}
