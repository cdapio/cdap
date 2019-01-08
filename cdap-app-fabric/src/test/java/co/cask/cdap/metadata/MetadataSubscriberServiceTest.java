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

package co.cask.cdap.metadata;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.api.workflow.NodeStatus;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metadata.MetadataRecord;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.config.PreferencesService;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.lineage.LineageStoreReader;
import co.cask.cdap.data2.metadata.lineage.field.EndPointField;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import co.cask.cdap.data2.metadata.lineage.field.FieldLineageReader;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.SystemMetadataProvider;
import co.cask.cdap.data2.metadata.writer.FieldLineageWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.metadata.writer.MessagingLineageWriter;
import co.cask.cdap.data2.metadata.writer.MessagingMetadataPublisher;
import co.cask.cdap.data2.metadata.writer.MetadataOperation;
import co.cask.cdap.data2.metadata.writer.MetadataPublisher;
import co.cask.cdap.data2.registry.MessagingUsageWriter;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.registry.UsageWriter;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.app.runtime.workflow.MessagingWorkflowStateWriter;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowStateWriter;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.profile.AdminEventPublisher;
import co.cask.cdap.internal.profile.ProfileService;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.proto.metadata.lineage.ProgramRunOperations;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.scheduler.ProgramScheduleService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;

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

    // Write out some lineage information
    LineageWriter lineageWriter = getInjector().getInstance(MessagingLineageWriter.class);
    ProgramRunId run1 = flow1.run(RunIds.generate());
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    lineageWriter.addAccess(run1, dataset2, AccessType.WRITE);

    LineageStoreReader lineageReader = getInjector().getInstance(LineageStoreReader.class);

    // Try to read lineage, which should be empty since we haven't start the MetadataSubscriberService yet.
    Set<NamespacedEntityId> entities = lineageReader.getEntitiesForRun(run1);
    Assert.assertTrue(entities.isEmpty());

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

    // Emit one more lineage
    lineageWriter.addAccess(run1, stream1, AccessType.UNKNOWN, flowlet1);
    expectedLineage.add(stream1);
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

    // Emit one more usage
    usageWriter.register(flow1, stream1);
    expectedUsage.clear();
    expectedUsage.add(stream1);
    Tasks.waitFor(true, () -> expectedUsage.equals(usageRegistry.getStreams(flow1)),
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
  public void testMetadata() throws InterruptedException, TimeoutException, ExecutionException {
    ProgramRunId workflowRunId = workflow1.run(RunIds.generate());
    MetadataEntity entity = MetadataEntity.ofDataset("myns", "myds");

    // Try to read, should have nothing
    MetadataStore metadataStore = getInjector().getInstance(MetadataStore.class);
    MetadataRecord meta = metadataStore.getMetadata(MetadataScope.USER, entity);
    Assert.assertTrue(meta.getProperties().isEmpty());
    Assert.assertTrue(meta.getTags().isEmpty());

    MetadataPublisher metadataPublisher = getInjector().getInstance(MessagingMetadataPublisher.class);
    final String descriptionKey = SystemMetadataProvider.DESCRIPTION_KEY;
    final String creationTimeKey = SystemMetadataProvider.CREATION_TIME_KEY;

    // publish a create event
    Map<String, String> props = ImmutableMap.of("x", "y", descriptionKey, "desc1", creationTimeKey, "123456");
    Set<String> tags = ImmutableSet.of("sometag");
    metadataPublisher.publish(NamespaceId.SYSTEM, new MetadataOperation.Create(entity, props, tags));

    // wait until meta data is written
    waitForSystemMetadata(entity, metadataStore, 3, 1);

    // validate correctness of meta data after create
    meta = metadataStore.getMetadata(MetadataScope.SYSTEM, entity);
    Assert.assertEquals(props, meta.getProperties());
    Assert.assertEquals(tags, meta.getTags());

    // publish another create event with different create time, no description, different tags
    Set<String> tags2 = ImmutableSet.of("another", "two");
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Create(
      entity, ImmutableMap.of(creationTimeKey, "9876543", "new", "prop"), tags2));
    // wait until meta data is written
    waitForSystemMetadata(entity, metadataStore, 3, 2);

    // validate correctness of meta data: creation time and description unchanged, other new property there
    meta = metadataStore.getMetadata(MetadataScope.SYSTEM, entity);
    Assert.assertEquals(ImmutableMap.of(creationTimeKey, "123456", descriptionKey, "desc1", "new", "prop"),
                        meta.getProperties());
    Assert.assertEquals(tags2, meta.getTags());

    // publish another create event without create time, different description, no tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Create(
      entity, ImmutableMap.of(descriptionKey, "some"), Collections.emptySet()));
    // wait until meta data is written
    waitForSystemMetadata(entity, metadataStore, 2, 0);

    // validate correctness of meta data: same creation time, updated description and other props and tags
    meta = metadataStore.getMetadata(MetadataScope.SYSTEM, entity);
    Assert.assertEquals(ImmutableMap.of(creationTimeKey, "123456", descriptionKey, "some"), meta.getProperties());
    Assert.assertEquals(Collections.emptySet(), meta.getTags());

    // publish metadata put
    Map<String, String> propertiesToAdd = ImmutableMap.of("a", "x", "b", "z");
    Set<String> tagsToAdd = ImmutableSet.of("t1", "t2");
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Put(entity, propertiesToAdd, tagsToAdd));

    // wait until meta data is written
    waitForMetadata(entity, metadataStore, 2, 2);

    // validate correctness of meta data written
    meta = metadataStore.getMetadata(MetadataScope.USER, entity);
    Assert.assertEquals(propertiesToAdd, meta.getProperties());
    Assert.assertEquals(tagsToAdd, meta.getTags());

    // publish metadata delete
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Delete(
      entity, Collections.singleton("a"), ImmutableSet.of("t1", "t3")));

    // wait until meta data is written
    waitForMetadata(entity, metadataStore, 1, 1);

    // validate correctness of meta data after delete
    meta = metadataStore.getMetadata(MetadataScope.USER, entity);
    Assert.assertEquals(ImmutableMap.of("b", "z"), meta.getProperties());
    Assert.assertEquals(ImmutableSet.of("t2"), meta.getTags());

    // publish metadata put properties
    metadataPublisher.publish(workflowRunId,
                              new MetadataOperation.Put(entity, propertiesToAdd, Collections.emptySet()));

    // wait until meta data is written
    // one of the property key already exist so for that value will be just overwritten hence size is 2
    waitForMetadata(entity, metadataStore, 2, 1);

    // publish metadata put tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Put(entity, Collections.emptyMap(), tagsToAdd));

    // wait until meta data is written
    // one of the tags already exists hence size is 2
    waitForMetadata(entity, metadataStore, 2, 2);

    // publish delete all properties
    metadataPublisher.publish(workflowRunId, new MetadataOperation.DeleteAllProperties(entity));

    // wait until meta data is written
    waitForMetadata(entity, metadataStore, 0, 2);

    // publish delete all tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.DeleteAllTags(entity));

    // wait until meta data is written
    waitForMetadata(entity, metadataStore, 0, 0);

    // publish metadata put tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Put(entity, propertiesToAdd, tagsToAdd));

    // wait until meta data is written
    waitForMetadata(entity, metadataStore, 2, 2);

    // publish delete all
    metadataPublisher.publish(workflowRunId, new MetadataOperation.DeleteAll(entity));

    // wait until meta data is written
    waitForMetadata(entity, metadataStore, 0, 0);

    // publish metadata put tags
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Put(entity, propertiesToAdd, tagsToAdd));

    // wait until meta data is written
    waitForMetadata(entity, metadataStore, 2, 2);

    // publish drop entity
    metadataPublisher.publish(workflowRunId, new MetadataOperation.Drop(entity));
    // wait until meta data is deleted
    waitForSystemMetadata(entity, metadataStore, 0, 0);
    waitForMetadata(entity, metadataStore, 0, 0);
  }

  @Test
  public void testProfileMetadata() throws Exception {
    Injector injector = getInjector();

    ApplicationSpecification appSpec = Specifications.from(new AppWithWorkflow());
    ApplicationId appId = NamespaceId.DEFAULT.app(appSpec.getName());
    ProgramId workflowId = appId.workflow("SampleWorkflow");
    ScheduleId scheduleId = appId.schedule("tsched1");

    // get the mds should be empty property since we haven't started the MetadataSubscriberService
    MetadataStore mds = injector.getInstance(MetadataStore.class);
    Assert.assertEquals(Collections.emptyMap(), mds.getProperties(workflowId.toMetadataEntity()));
    Assert.assertEquals(Collections.emptyMap(), mds.getProperties(scheduleId.toMetadataEntity()));

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
                    () -> mds.getProperties(workflowId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(),
                    () -> mds.getProperties(scheduleId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);


      // set default namespace to use my profile
      preferencesService.setProperties(NamespaceId.DEFAULT,
                                       Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:MyProfile"));

      // Verify the workflow profile metadata is updated to my profile
      Tasks.waitFor(myProfile.getScopedName(), () -> mds.getProperties(workflowId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to my profile
      Tasks.waitFor(myProfile.getScopedName(), () -> mds.getProperties(scheduleId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // set app level to use my profile 2
      preferencesService.setProperties(appId,
                                       Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:MyProfile2"));

      // set instance level to system profile
      preferencesService.setProperties(Collections.singletonMap(SystemArguments.PROFILE_NAME,
                                                                ProfileId.NATIVE.getScopedName()));

      // Verify the workflow profile metadata is updated to MyProfile2 which is at app level
      Tasks.waitFor(myProfile2.getScopedName(), () -> mds.getProperties(workflowId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to MyProfile2 which is at app level
      Tasks.waitFor(myProfile2.getScopedName(), () -> mds.getProperties(scheduleId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // remove the preferences at instance level, should not affect the metadata
      preferencesService.deleteProperties();

      // Verify the workflow profile metadata is updated to default profile
      Tasks.waitFor(myProfile2.getScopedName(), () -> mds.getProperties(workflowId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to default profile
      Tasks.waitFor(myProfile2.getScopedName(), () -> mds.getProperties(scheduleId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // remove app level pref should let the programs/schedules use ns level pref
      preferencesService.deleteProperties(appId);

      // Verify the workflow profile metadata is updated to MyProfile
      Tasks.waitFor(myProfile.getScopedName(), () -> mds.getProperties(workflowId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to MyProfile
      Tasks.waitFor(myProfile.getScopedName(), () -> mds.getProperties(scheduleId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // remove ns level pref so no pref is there
      preferencesService.deleteProperties(NamespaceId.DEFAULT);

      // Verify the workflow profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(),
                    () -> mds.getProperties(workflowId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
      // Verify the schedule profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(),
                    () -> mds.getProperties(scheduleId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      // stop and clean up the store
      preferencesService.deleteProperties(NamespaceId.DEFAULT);
      preferencesService.deleteProperties();
      preferencesService.deleteProperties(appId);
      store.removeAllApplications(NamespaceId.DEFAULT);
      scheduleService.delete(scheduleId);
      profileService.disableProfile(myProfile);
      profileService.disableProfile(myProfile2);
      profileService.deleteAllProfiles(myProfile.getNamespaceId());
      mds.removeMetadata(workflowId.toMetadataEntity());
      mds.removeMetadata(scheduleId.toMetadataEntity());
    }
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
    MetadataStore mds = injector.getInstance(MetadataStore.class);
    Assert.assertEquals(Collections.emptyMap(), mds.getProperties(workflowId.toMetadataEntity()));

    Store store = injector.getInstance(DefaultStore.class);
    store.addApplication(appId, appSpec);

    // set default namespace to use the profile, since now MetadataSubscriberService is not started,
    // it should not affect the mds
    PreferencesService preferencesService = injector.getInstance(PreferencesService.class);
    preferencesService.setProperties(NamespaceId.DEFAULT,
                                     Collections.singletonMap(SystemArguments.PROFILE_NAME, "USER:MyProfile"));

    try {
      // Verify the workflow profile metadata is updated to my profile
      Tasks.waitFor(myProfile.getScopedName(), () -> mds.getProperties(workflowId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // Set the property without profile is a replacement of the preference, so it is same as deletion of the profile
      preferencesService.setProperties(NamespaceId.DEFAULT, Collections.emptyMap());

      // Verify the workflow profile metadata is updated to default profile
      Tasks.waitFor(ProfileId.NATIVE.getScopedName(),
                    () -> mds.getProperties(workflowId.toMetadataEntity()).get("profile"),
                    10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
    } finally {
      // stop and clean up the store
      preferencesService.deleteProperties(NamespaceId.DEFAULT);
      store.removeAllApplications(NamespaceId.DEFAULT);
      profileService.disableProfile(myProfile);
      profileService.deleteProfile(myProfile);
      mds.removeMetadata(workflowId.toMetadataEntity());
    }
  }
  private void waitForMetadata(MetadataEntity entity, MetadataStore metadataStore, int propertiesSize,
                               int tagSize) throws TimeoutException, InterruptedException, ExecutionException {
    Tasks.waitFor(true, () -> {
      MetadataRecord record = metadataStore.getMetadata(MetadataScope.USER, entity);
      return record.getProperties().size() == propertiesSize && record.getTags().size() == tagSize;
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  private void waitForSystemMetadata(MetadataEntity entity, MetadataStore metadataStore, int propertiesSize,
                                     int tagSize) throws TimeoutException, InterruptedException, ExecutionException {
    Tasks.waitFor(true, () -> {
      MetadataRecord record = metadataStore.getMetadata(MetadataScope.SYSTEM, entity);
      return record.getProperties().size() == propertiesSize && record.getTags().size() == tagSize;
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
    MetadataStore mds = injector.getInstance(MetadataStore.class);

    // use an app with all program types to get all specification tested
    ApplicationId appId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    ProgramId workflowId = appId.workflow(AllProgramsApp.NoOpWorkflow.NAME);
    // generate an app spec from the application, this app spec will have the transient inputTypes and outputTypes
    // in FlowletDefinition, we need to generate the schema from these two fields.
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    // We need to use the adapter to generate a true app spec which has the inputs and outputs field generated for
    // FlowletDefinition, otherwise gson will ignore the transient inputTypes and outputTypes fields and
    // make inputs and outputs field become null, which loses the intent of this test,
    // because we want to test if the subsciber is able to deserialize the Schema in FlowletDefinition
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    String jsonString = adapter.toJson(appSpec);
    ApplicationSpecification trueSpec = adapter.fromJson(jsonString);

    // need to put metadata on workflow since we currently only set or delete workflow metadata
    mds.setProperties(MetadataScope.SYSTEM, workflowId.toMetadataEntity(),
                      Collections.singletonMap("profile", ProfileId.NATIVE.getScopedName()));
    Assert.assertEquals(ProfileId.NATIVE.getScopedName(),
                        mds.getProperties(workflowId.toMetadataEntity()).get("profile"));

    // publish app deletion message
    publisher.publishAppDeletion(appId, trueSpec);

    // Verify the workflow profile metadata is removed because of the publish app deletion message
    Tasks.waitFor(Collections.emptyMap(), () -> mds.getProperties(workflowId.toMetadataEntity()),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }
}
