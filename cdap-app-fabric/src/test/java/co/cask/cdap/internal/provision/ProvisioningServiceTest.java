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

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.Requirements;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.internal.pipeline.PluginRequirement;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.runtime.spi.provisioner.Capabilities;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.security.FakeSecureStore;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for Provisioning Service.
 */
public class ProvisioningServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static ProvisioningService provisioningService;
  private static TransactionManager txManager;
  private static DatasetService datasetService;
  private static MessagingService messagingService;
  private static ProvisionerStore provisionerStore;

  @BeforeClass
  public static void setupClass() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class), structuredTableRegistry);
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    messagingService = injector.getInstance(MessagingService.class);
    provisionerStore = injector.getInstance(ProvisionerStore.class);
    if (messagingService instanceof Service) {
      ((Service) messagingService).startAndWait();
    }
    try {
      // Define all StructuredTable before starting any services that need StructuredTable
      StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class),
                                      injector.getInstance(StructuredTableRegistry.class));
    } catch (IOException | TableAlreadyExistsException e) {
      throw new RuntimeException("Unable to create the system tables.", e);
    }
    provisioningService = injector.getInstance(ProvisioningService.class);
    provisioningService.startAndWait();

  }

  @AfterClass
  public static void cleanupClass() {
    provisioningService.stopAndWait();
    datasetService.stopAndWait();
    txManager.stopAndWait();
    if (messagingService instanceof Service) {
      ((Service) messagingService).stopAndWait();
    }
  }

  @Test
  public void testGetClusterStatus() throws Exception {
    TaskFields taskFields = createTaskInfo(new MockProvisioner.PropertyBuilder()
                                             .setFirstClusterStatus(ClusterStatus.RUNNING)
                                             .failRetryablyEveryN(2)
                                             .build());

    Cluster cluster = new Cluster("test", ClusterStatus.NOT_EXISTS, Collections.emptyList(), Collections.emptyMap());
    Assert.assertEquals(ClusterStatus.RUNNING,
                        provisioningService.getClusterStatus(taskFields.programRunId, taskFields.programOptions,
                                                             cluster, "cdap"));
  }

  @Test(expected = Exception.class)
  public void testGetClusterStatusFailure() throws Exception {
    TaskFields taskFields = createTaskInfo(new MockProvisioner.PropertyBuilder()
                                             .setFirstClusterStatus(ClusterStatus.RUNNING)
                                             .failGet()
                                             .build());

    Cluster cluster = new Cluster("test", ClusterStatus.NOT_EXISTS, Collections.emptyList(), Collections.emptyMap());
    provisioningService.getClusterStatus(taskFields.programRunId, taskFields.programOptions, cluster, "cdap");
  }

  @Test
  public void testGetSpecs() {
    Collection<ProvisionerDetail> specs = provisioningService.getProvisionerDetails();
    Assert.assertEquals(1, specs.size());

    ProvisionerSpecification spec = new MockProvisioner().getSpec();
    ProvisionerDetail expected = new ProvisionerDetail(spec.getName(), spec.getLabel(),
                                                       spec.getDescription(), new ArrayList<>(), null, false);
    Assert.assertEquals(expected, specs.iterator().next());

    Assert.assertEquals(expected, provisioningService.getProvisionerDetail(MockProvisioner.NAME));
    Assert.assertNull(provisioningService.getProvisionerDetail("abc"));
  }

  @Test
  public void testNoErrors() throws Exception {
    ProvisionerInfo provisionerInfo = new MockProvisioner.PropertyBuilder().build();
    TaskFields taskFields = testProvision(ProvisioningOp.Status.CREATED, provisionerInfo);
    testDeprovision(taskFields.programRunId, ProvisioningOp.Status.DELETED);
  }

  @Test
  public void testRetryableFailures() throws Exception {
    // will throw a retryable exception every other method call
    ProvisionerInfo provisionerInfo = new MockProvisioner.PropertyBuilder().failRetryablyEveryN(2).build();
    TaskFields taskFields = testProvision(ProvisioningOp.Status.CREATED, provisionerInfo);
    testDeprovision(taskFields.programRunId, ProvisioningOp.Status.DELETED);
  }

  @Test
  public void testProvisionCreateFailure() throws Exception {
    testProvision(ProvisioningOp.Status.FAILED, new MockProvisioner.PropertyBuilder().failCreate().build());
  }

  @Test
  public void testProvisionPollFailure() throws Exception {
    testProvision(ProvisioningOp.Status.FAILED, new MockProvisioner.PropertyBuilder().failGet().build());
  }

  @Test
  public void testProvisionInitFailure() throws Exception {
    testProvision(ProvisioningOp.Status.FAILED, new MockProvisioner.PropertyBuilder().failInit().build());
  }

  @Test
  public void testProvisionCreateRetry() throws Exception {
    // simulates cluster create, then when polling, cluster status goes to a state that requires
    // that the cluster be deleted, and create retried
    testProvision(ProvisioningOp.Status.CREATED,
                  new MockProvisioner.PropertyBuilder()
                             .setFirstClusterStatus(ClusterStatus.FAILED)
                             .failRetryablyEveryN(2)
                             .build());
    testProvision(ProvisioningOp.Status.CREATED,
                  new MockProvisioner.PropertyBuilder()
                             .setFirstClusterStatus(ClusterStatus.DELETING)
                             .failRetryablyEveryN(2)
                             .build());
    testProvision(ProvisioningOp.Status.CREATED,
                  new MockProvisioner.PropertyBuilder()
                             .setFirstClusterStatus(ClusterStatus.NOT_EXISTS)
                             .failRetryablyEveryN(2)
                             .build());
  }

  // should be able to 'deprovision' a cluster that couldn't be created
  @Test
  public void testClusterCreateFailure() throws Exception {
    ProgramRunId programRunId = testProvision(ProvisioningOp.Status.FAILED,
                                              new MockProvisioner.PropertyBuilder().failCreate().build()).programRunId;

    // TODO: CDAP-14909 once all stores are migrated update the test with transactionals
    Runnable task = provisioningService.deprovision(programRunId);
    task.run();

    // task state should have been cleaned up
    ProvisioningTaskInfo taskInfo = provisionerStore.
      getTaskInfo(new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.PROVISION));
    Assert.assertNull("provision task info was not cleaned up", taskInfo);
    taskInfo =  provisionerStore.getTaskInfo(new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.DEPROVISION));
    Assert.assertNull("deprovision task info was not cleaned up", taskInfo);
  }

  @Test
  public void testDeprovisionFailure() throws Exception {
    TaskFields taskFields = testProvision(ProvisioningOp.Status.CREATED,
                                          new MockProvisioner.PropertyBuilder().failDelete().build());
    testDeprovision(taskFields.programRunId, ProvisioningOp.Status.FAILED);
  }

  @Test
  public void testScanForTasks() throws Exception {
    // write state for a provision operation that is polling for the cluster to be created
    TaskFields taskFields = createTaskInfo(new MockProvisioner.PropertyBuilder().build());

    ProvisioningOp op = new ProvisioningOp(ProvisioningOp.Type.PROVISION, ProvisioningOp.Status.POLLING_CREATE);
    Cluster cluster = new Cluster("name", ClusterStatus.CREATING, Collections.emptyList(), Collections.emptyMap());
    ProvisioningTaskInfo taskInfo = new ProvisioningTaskInfo(taskFields.programRunId, taskFields.programDescriptor,
                                                             taskFields.programOptions, Collections.emptyMap(),
                                                             MockProvisioner.NAME, "Bob",
                                                             op, Locations.toLocation(TEMP_FOLDER.newFolder()).toURI(),
                                                             cluster);

    provisionerStore.putTaskInfo(taskInfo);
    provisioningService.resumeTasks(t -> { });

    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(taskFields.programRunId, ProvisioningOp.Type.PROVISION);
    waitForExpectedProvisioningState(taskKey, ProvisioningOp.Status.CREATED);
  }

  @Test
  public void testCancelProvision() throws InterruptedException, ExecutionException, TimeoutException, IOException {
    ProvisionerInfo provisionerInfo = new MockProvisioner.PropertyBuilder().waitCreate(1, TimeUnit.MINUTES).build();
    TaskFields taskFields = createTaskInfo(provisionerInfo);
    ProvisionRequest provisionRequest = new ProvisionRequest(taskFields.programRunId, taskFields.programOptions,
                                                             taskFields.programDescriptor, "Bob");

    // TODO: CDAP-14909 once all stores are migrated update the test with transactionals
    Runnable task = provisioningService.provision(provisionRequest);
    task.run();

    Assert.assertTrue(provisioningService.cancelProvisionTask(taskFields.programRunId).isPresent());

    // check that the state of the task is cancelled
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(taskFields.programRunId, ProvisioningOp.Type.PROVISION);
    waitForExpectedProvisioningState(taskKey, ProvisioningOp.Status.CANCELLED);
  }

  @Test
  public void testCancelDeprovision() throws Exception {
    ProvisionerInfo provisionerInfo = new MockProvisioner.PropertyBuilder().waitDelete(1, TimeUnit.MINUTES).build();
    TaskFields taskFields = testProvision(ProvisioningOp.Status.CREATED, provisionerInfo);

    // TODO: CDAP-14909 once all stores are migrated update the test with transactionals
    Runnable task = provisioningService.deprovision(taskFields.programRunId, t -> { });
    task.run();
    Assert.assertTrue(provisioningService.cancelDeprovisionTask(taskFields.programRunId).isPresent());

    // check that the state of the task is cancelled
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(taskFields.programRunId, ProvisioningOp.Type.DEPROVISION);
    waitForExpectedProvisioningState(taskKey, ProvisioningOp.Status.CANCELLED);
  }

  private TaskFields testProvision(ProvisioningOp.Status expectedState, ProvisionerInfo provisionerInfo)
    throws InterruptedException, ExecutionException, TimeoutException, IOException {
    TaskFields taskFields = createTaskInfo(provisionerInfo);
    ProvisionRequest provisionRequest = new ProvisionRequest(taskFields.programRunId, taskFields.programOptions,
                                                               taskFields.programDescriptor, "Bob");
    // TODO: CDAP-14909 once all stores are migrated update the test with transactionals
    Runnable task = provisioningService.provision(provisionRequest);
    task.run();

    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(taskFields.programRunId, ProvisioningOp.Type.PROVISION);
    waitForExpectedProvisioningState(taskKey, expectedState);
    return taskFields;
  }

  private void testDeprovision(ProgramRunId programRunId, ProvisioningOp.Status expectedState)
    throws InterruptedException, ExecutionException, TimeoutException, IOException {
    // TODO: CDAP-14909 once all stores are migrated update the test with transactionals
    Runnable task = provisioningService.deprovision(programRunId, t -> { });
    task.run();
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.DEPROVISION);
    waitForExpectedProvisioningState(taskKey, expectedState);
  }

  private TaskFields createTaskInfo(ProvisionerInfo provisionerInfo) {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("wf").run(RunIds.generate());
    Map<String, String> systemArgs = new HashMap<>();
    Map<String, String> userArgs = new HashMap<>();

    Profile profile = new Profile(ProfileId.NATIVE.getProfile(), "label", "desc", provisionerInfo);
    SystemArguments.addProfileArgs(systemArgs, profile);
    ProgramOptions programOptions = new SimpleProgramOptions(programRunId.getParent(),
                                                             new BasicArguments(systemArgs),
                                                             new BasicArguments(userArgs));
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("testArtifact", "1.0").toApiArtifactId();
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
      "name", "1.0.0", "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programRunId.getParent(), appSpec);

    return new TaskFields(programDescriptor, programOptions, programRunId);
  }

  private void waitForExpectedProvisioningState(ProvisioningTaskKey taskKey, ProvisioningOp.Status expectedState)
    throws InterruptedException, ExecutionException, TimeoutException, IOException {
    Tasks.waitFor(expectedState, () ->  {
      ProvisioningTaskInfo provisioningTaskInfo = provisionerStore.getTaskInfo(taskKey);
      return provisioningTaskInfo == null ? null : provisioningTaskInfo.getProvisioningOp().getStatus();
    }, 60, TimeUnit.SECONDS);
  }

  @Test
  public void testSecureMacroEvaluation() {
    String key = "key";
    String keycontent = "somecontent";
    Map<String, String> properties = ImmutableMap.of("x", "${secure(" + key + ")}");

    SecureStore secureStore = FakeSecureStore.builder()
      .putValue(NamespaceId.DEFAULT.getNamespace(), key, keycontent)
      .build();

    Map<String, String> evaluated = ProvisioningService.evaluateMacros(secureStore, "Bob",
                                                                       NamespaceId.DEFAULT.getNamespace(), properties);
    Assert.assertEquals(keycontent, evaluated.get("x"));
  }

  @Test
  public void testUnfulfilledRequirements() {
    Capabilities provisionerCapabilities = new Capabilities(ImmutableSet.of(Table.TYPE));
    Set<PluginRequirement> requirements =
      ImmutableSet.of(new PluginRequirement("source1", "batchsource",
                                            new Requirements(ImmutableSet.of(Table.TYPE, "unicorn"))),
                      new PluginRequirement("sink1", "batchsink",
                                            new Requirements(ImmutableSet.of(Table.TYPE, "dragon"))));

    Set<PluginRequirement> expectedUnfulfilledRequirements = ImmutableSet.of(
      new PluginRequirement("source1", "batchsource", new Requirements(ImmutableSet.of("unicorn"))),
      new PluginRequirement("sink1", "batchsink", new Requirements(ImmutableSet.of("dragon")))
    );

    assertRequirementFulfillment(provisionerCapabilities, requirements, expectedUnfulfilledRequirements);

    // check when there are multiple plugins with same name but different type
    requirements = ImmutableSet.of(new PluginRequirement("source1", "batchsource",
                                                         new Requirements(ImmutableSet.of(Table.TYPE, "unicorn"))),
                                   new PluginRequirement("sink1", "batchsink",
                                                               new Requirements(ImmutableSet.of(Table.TYPE, "dragon"))),
                                   new PluginRequirement("sink1", "anothersink",
                                                         new Requirements(ImmutableSet.of(Table.TYPE, "narwhal"))));

    expectedUnfulfilledRequirements = ImmutableSet.of(
      new PluginRequirement("source1", "batchsource", new Requirements(ImmutableSet.of("unicorn"))),
      new PluginRequirement("sink1", "batchsink", new Requirements(ImmutableSet.of("dragon"))),
      new PluginRequirement("sink1", "anothersink", new Requirements(ImmutableSet.of("narwhal"))
    ));
    assertRequirementFulfillment(provisionerCapabilities, requirements, expectedUnfulfilledRequirements);

    // check when provisioner does not have any specified capability
    provisionerCapabilities = Capabilities.EMPTY;
    assertRequirementFulfillment(provisionerCapabilities, requirements, requirements);
  }

  @Test
  public void testFulfilledRequirements() {
    Capabilities provisionerCapabilities = new Capabilities(ImmutableSet.of(Table.TYPE));
    Set<PluginRequirement> requirements =
      ImmutableSet.of(new PluginRequirement("source1", "batchsource", new Requirements(Collections.emptySet())),
                      new PluginRequirement("sink1", "batchsink", new Requirements(ImmutableSet.of(Table.TYPE))));

    // there should not be any incapability
    assertRequirementFulfillment(provisionerCapabilities, requirements, Collections.emptySet());

    provisionerCapabilities = new Capabilities(ImmutableSet.of(Table.TYPE));
    requirements = ImmutableSet.of(new PluginRequirement("source1", "batchsource",
                                                         new Requirements(ImmutableSet.of(Table.TYPE))),
                                   new PluginRequirement("sink1", "batchsink",
                                                         new Requirements(ImmutableSet.of(Table.TYPE))));
    // there should not be any incapability
    assertRequirementFulfillment(provisionerCapabilities, requirements, Collections.emptySet());
  }

  private void assertRequirementFulfillment(Capabilities provisionerCapabilities,
                                            Set<PluginRequirement> pluginRequirements,
                                            Set<PluginRequirement> expectedUnfulfilledRequirements) {
    Set<PluginRequirement> unfulfilledRequirements =
      provisioningService.getUnfulfilledRequirements(provisionerCapabilities, pluginRequirements);
    Assert.assertEquals(expectedUnfulfilledRequirements, unfulfilledRequirements);
  }

  @Test
  public void testGroupByRequirement() {
    Set<PluginRequirement> requirements =
      ImmutableSet.of(new PluginRequirement("source1", "batchsource",
                                            new Requirements(ImmutableSet.of(Table.TYPE, "unicorn"))),
                      new PluginRequirement("sink1", "batchsink", new Requirements(ImmutableSet.of(Table.TYPE,
                                                                                                   "dragon"))),
                      new PluginRequirement("sink1", "anothersink",
                                            new Requirements(ImmutableSet.of(Table.TYPE, "narwhal"))));
    Map<String, Set<String>> pluginGroupedByRequirement = provisioningService.groupByRequirement(requirements);

    Assert.assertEquals(4, pluginGroupedByRequirement.size());
    Assert.assertEquals(ImmutableSet.of("batchsource:source1", "batchsink:sink1", "anothersink:sink1"),
                        pluginGroupedByRequirement.get(Table.TYPE));
    Assert.assertEquals(ImmutableSet.of("batchsource:source1"), pluginGroupedByRequirement.get("unicorn"));
    Assert.assertEquals(ImmutableSet.of("batchsink:sink1"), pluginGroupedByRequirement.get("dragon"));
    Assert.assertEquals(ImmutableSet.of("anothersink:sink1"), pluginGroupedByRequirement.get("narwhal"));

    // test empty
    pluginGroupedByRequirement = provisioningService.groupByRequirement(Collections.emptySet());
    Assert.assertTrue(pluginGroupedByRequirement.isEmpty());
  }

  /**
   * Holds program run task information.
   */
  private static class TaskFields {
    private final ProgramDescriptor programDescriptor;
    private final ProgramOptions programOptions;
    private final ProgramRunId programRunId;

    TaskFields(ProgramDescriptor programDescriptor, ProgramOptions programOptions, ProgramRunId programRunId) {
      this.programDescriptor = programDescriptor;
      this.programOptions = programOptions;
      this.programRunId = programRunId;
    }
  }
}
