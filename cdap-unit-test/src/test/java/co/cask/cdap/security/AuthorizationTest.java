/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.security;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.spark.stream.TestSparkCrossNSDatasetApp;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ArtifactManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ProgramManager;
import co.cask.cdap.test.ScheduleManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdap.test.app.AppWithSchedule;
import co.cask.cdap.test.app.CrossNsDatasetAccessApp;
import co.cask.cdap.test.app.DatasetCrossNSAccessWithMAPApp;
import co.cask.cdap.test.app.DummyApp;
import co.cask.cdap.test.app.StreamAuthApp;
import co.cask.cdap.test.artifacts.plugins.ToStringPlugin;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit tests with authorization enabled.
 */
public class AuthorizationTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(
    Constants.Explore.EXPLORE_ENABLED, false,
    Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0
  );
  private static final EnumSet<Action> ALL_ACTIONS = EnumSet.allOf(Action.class);

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId AUTH_NAMESPACE = new NamespaceId("authorization");
  private static final NamespaceMeta AUTH_NAMESPACE_META =
    new NamespaceMeta.Builder().setName(AUTH_NAMESPACE.getNamespace()).build();

  private static String oldUser;
  private static Set<EntityId> cleanUpEntities;

  /**
   * An {@link ExternalResource} that wraps a {@link TemporaryFolder} and {@link TestConfiguration} to execute them in
   * a chain.
   */
  private static final class AuthTestConf extends ExternalResource {
    private final TemporaryFolder tmpFolder = new TemporaryFolder();
    private TestConfiguration testConf;

    @Override
    public Statement apply(final Statement base, final Description description) {
      // Apply the TemporaryFolder on a Statement that creates a TestConfiguration and applies on base
      return tmpFolder.apply(new Statement() {
        @Override
        public void evaluate() throws Throwable {
          testConf = new TestConfiguration(getAuthConfigs(tmpFolder.newFolder()));
          testConf.apply(base, description).evaluate();
        }
      }, description);
    }

    private static String[] getAuthConfigs(File tmpDir) throws IOException {
      LocationFactory locationFactory = new LocalLocationFactory(tmpDir);
      Location authExtensionJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
      return new String[]{
        Constants.Security.ENABLED, "true",
        Constants.Security.Authorization.ENABLED, "true",
        Constants.Security.Authorization.EXTENSION_JAR_PATH, authExtensionJar.toURI().getPath(),
        // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
        Constants.Security.KERBEROS_ENABLED, "false",
        // this is needed since now DefaultAuthorizationEnforcer expects this non-null
        Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, UserGroupInformation.getLoginUser().getShortUserName()
      };
    }
  }

  @ClassRule
  public static final AuthTestConf AUTH_TEST_CONF = new AuthTestConf();

  @BeforeClass
  public static void setup() {
    oldUser = SecurityRequestContext.getUserId();
  }

  @Before
  public void setupTest() throws Exception {
    Assert.assertEquals(ImmutableSet.<Privilege>of(), getAuthorizer().listPrivileges(ALICE));
    SecurityRequestContext.setUserId(ALICE.getName());
    cleanUpEntities = new HashSet<>();
  }

  @Test
  public void testNamespaces() throws Exception {
    NamespaceAdmin namespaceAdmin = getNamespaceAdmin();
    Authorizer authorizer = getAuthorizer();
    try {
      namespaceAdmin.create(AUTH_NAMESPACE_META);
      Assert.fail("Namespace create should have failed because alice is not authorized on " + AUTH_NAMESPACE);
    } catch (UnauthorizedException expected) {
      // expected
    }
    createAuthNamespace();
    System.out.println("#### " + namespaceAdmin.list().size());
    Assert.assertTrue(namespaceAdmin.list().contains(AUTH_NAMESPACE_META));
    namespaceAdmin.get(AUTH_NAMESPACE);
    // revoke privileges
    revokeAndAssertSuccess(AUTH_NAMESPACE);
    try {
      Assert.assertTrue(namespaceAdmin.list().isEmpty());
      namespaceAdmin.exists(AUTH_NAMESPACE);
      Assert.fail("Namespace existence check should fail since the privilege of alice has been revoked");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant privileges again
    grantAndAssertSuccess(AUTH_NAMESPACE, ALICE, ImmutableSet.of(Action.ADMIN));
    namespaceAdmin.exists(AUTH_NAMESPACE);
    Assert.assertEquals(ImmutableSet.of(new Privilege(AUTH_NAMESPACE, Action.ADMIN)), authorizer.listPrivileges(ALICE));
    NamespaceMeta updated = new NamespaceMeta.Builder(AUTH_NAMESPACE_META).setDescription("new desc").build();
    namespaceAdmin.updateProperties(AUTH_NAMESPACE, updated);
    Assert.assertEquals(updated, namespaceAdmin.get(AUTH_NAMESPACE));
  }

  @Test
  @Category(SlowTests.class)
  public void testFlowStreamAuth() throws Exception {
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    // set up privilege to deploy the app
    setUpPrivilegeToDeployStreamAuthApp();
    StreamId streamId1 = AUTH_NAMESPACE.stream(StreamAuthApp.STREAM);
    StreamId streamId2 = AUTH_NAMESPACE.stream(StreamAuthApp.STREAM2);
    Map<EntityId, Set<Action>> additionalPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(streamId1, EnumSet.of(Action.READ, Action.WRITE))
      .put(streamId2, EnumSet.of(Action.READ, Action.WRITE))
      .put(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE), EnumSet.of(Action.READ, Action.WRITE))
      .put(AUTH_NAMESPACE.app(StreamAuthApp.APP).flow(StreamAuthApp.FLOW), EnumSet.of(Action.EXECUTE))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, additionalPrivileges);

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, StreamAuthApp.class);

    final FlowManager flowManager = appManager.getFlowManager(StreamAuthApp.FLOW);
    StreamManager streamManager = getStreamManager(streamId1);
    StreamManager streamManager2 = getStreamManager(streamId2);
    streamManager.send("Auth");
    flowManager.start();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> kvTable = getDataset(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE));
        return kvTable.get().read("Auth") != null;
      }
    }, 5, TimeUnit.SECONDS);
    flowManager.stop();
    flowManager.waitForRun(ProgramRunStatus.KILLED, 60, TimeUnit.SECONDS);

    // Now revoke the privileges for ALICE on the stream and grant her ADMIN and WRITE
    authorizer.revoke(streamId1, ALICE, EnumSet.allOf(Action.class));
    authorizer.grant(streamId1, ALICE, EnumSet.of(Action.WRITE, Action.ADMIN));
    streamManager.send("Security");
    streamManager2.send("Safety");
    try {
      flowManager.start();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    authorizer.grant(streamId1, ALICE, ImmutableSet.of(Action.READ));
    flowManager.start();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> kvTable = getDataset(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE));
        return kvTable.get().read("Security") != null;
      }
    }, 5, TimeUnit.SECONDS);
    TimeUnit.MILLISECONDS.sleep(10);
    flowManager.stop();
    flowManager.waitForRuns(ProgramRunStatus.KILLED, 2, 5, TimeUnit.SECONDS);
    appManager.delete();
  }

  @Test
  @Category(SlowTests.class)
  public void testWorkerStreamAuth() throws Exception {
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    setUpPrivilegeToDeployStreamAuthApp();

    StreamId streamId = AUTH_NAMESPACE.stream(StreamAuthApp.STREAM);
    Map<EntityId, Set<Action>> additionalPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(streamId, EnumSet.of(Action.READ, Action.WRITE))
      .put(AUTH_NAMESPACE.app(StreamAuthApp.APP).worker(StreamAuthApp.WORKER), EnumSet.of(Action.EXECUTE))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, additionalPrivileges);

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, StreamAuthApp.class);
    WorkerManager workerManager = appManager.getWorkerManager(StreamAuthApp.WORKER);
    workerManager.start();
    workerManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    StreamManager streamManager = getStreamManager(AUTH_NAMESPACE.stream(StreamAuthApp.STREAM));
    Assert.assertEquals(5, streamManager.getEvents(0, Long.MAX_VALUE, Integer.MAX_VALUE).size());

    // Now revoke write permission for Alice on that stream
    authorizer.revoke(streamId, ALICE, EnumSet.of(Action.WRITE));
    workerManager.start();
    workerManager.waitForRuns(ProgramRunStatus.FAILED, 1, 60, TimeUnit.SECONDS);

    Assert.assertEquals(5, streamManager.getEvents(0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    appManager.delete();
  }

  @Test
  @Category(SlowTests.class)
  public void testSparkStreamAuth() throws Exception {
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    setUpPrivilegeToDeployStreamAuthApp();
    StreamId streamId = AUTH_NAMESPACE.stream(StreamAuthApp.STREAM);
    Map<EntityId, Set<Action>> additionalPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(streamId, EnumSet.of(Action.READ, Action.WRITE))
      .put(AUTH_NAMESPACE.app(StreamAuthApp.APP).spark(StreamAuthApp.SPARK), EnumSet.of(Action.EXECUTE))
      .put(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE), EnumSet.of(Action.READ, Action.WRITE))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, additionalPrivileges);

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, StreamAuthApp.class);

    StreamManager streamManager = getStreamManager(streamId);
    streamManager.send("Hello");
    final SparkManager sparkManager = appManager.getSparkManager(StreamAuthApp.SPARK);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> kvManager = getDataset(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE));
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("Hello");
      Assert.assertArrayEquals(Bytes.toBytes("Hello"), value);
    }

    streamManager.send("World");
    // Revoke READ permission on STREAM for Alice
    authorizer.revoke(streamId, ALICE, EnumSet.of(Action.READ));
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.FAILED, 1, TimeUnit.MINUTES);

    kvManager = getDataset(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE));
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("World");
      Assert.assertNull(value);
    }

    // Grant ALICE READ permission on STREAM and now Spark job should run successfully
    authorizer.grant(streamId, ALICE, ImmutableSet.of(Action.READ));
    sparkManager.start();
    sparkManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 1, TimeUnit.MINUTES);

    kvManager = getDataset(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE));
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("World");
      Assert.assertArrayEquals(Bytes.toBytes("World"), value);
    }
    appManager.delete();
  }

  @Test
  @Category(SlowTests.class)
  public void testMRStreamAuth() throws Exception {
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    setUpPrivilegeToDeployStreamAuthApp();
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, StreamAuthApp.class);


    StreamId streamId = AUTH_NAMESPACE.stream(StreamAuthApp.STREAM);
    DatasetId datasetId = AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE);
    Map<EntityId, Set<Action>> additionalPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(streamId, EnumSet.of(Action.READ, Action.WRITE))
      .put(AUTH_NAMESPACE.app(StreamAuthApp.APP).mr(StreamAuthApp.MAPREDUCE), EnumSet.of(Action.EXECUTE))
      .put(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE), EnumSet.of(Action.READ, Action.WRITE))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, additionalPrivileges);

    StreamManager streamManager = getStreamManager(streamId);
    streamManager.send("Hello");
    final MapReduceManager mrManager = appManager.getMapReduceManager(StreamAuthApp.MAPREDUCE);
    mrManager.start();
    // Since Alice had the required permissions, she should be able to execute the MR job successfully
    mrManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> kvManager = getDataset(datasetId);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("Hello");
      Assert.assertArrayEquals(Bytes.toBytes("Hello"), value);
    }

    ProgramId mrId = AUTH_NAMESPACE.app(StreamAuthApp.APP).mr(StreamAuthApp.MAPREDUCE);
    authorizer.grant(mrId.getNamespaceId(), BOB, ImmutableSet.of(Action.ADMIN));

    authorizer.grant(mrId, BOB, EnumSet.of(Action.EXECUTE));
    authorizer.grant(AUTH_NAMESPACE.stream(StreamAuthApp.STREAM), BOB, EnumSet.of(Action.ADMIN));
    authorizer.grant(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE), BOB, EnumSet.of(Action.READ, Action.WRITE));
    streamManager.send("World");

    // Switch user to Bob. Note that he doesn't have READ access on the stream.
    SecurityRequestContext.setUserId(BOB.getName());
    mrManager.start();
    mrManager.waitForRun(ProgramRunStatus.FAILED, 1, TimeUnit.MINUTES);

    kvManager = getDataset(datasetId);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("World");
      Assert.assertNull(value);
    }

    // Now grant Bob, READ access on the stream. MR job should execute successfully now.
    authorizer.grant(AUTH_NAMESPACE.stream(StreamAuthApp.STREAM), BOB, ImmutableSet.of(Action.READ));
    mrManager.start();
    mrManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 1, TimeUnit.MINUTES);

    kvManager = getDataset(datasetId);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("World");
      Assert.assertEquals("World", Bytes.toString(value));
    }

    SecurityRequestContext.setUserId(ALICE.getName());
    appManager.delete();
  }

  @Test
  @Category(SlowTests.class)
  public void testStreams() throws Exception {
    createAuthNamespace();
    StreamId streamId = AUTH_NAMESPACE.stream("someStream");
    grantAndAssertSuccess(streamId, ALICE, EnumSet.allOf(Action.class));
    cleanUpEntities.add(streamId);

    // create stream as alice
    getStreamManager(streamId).createStream();

    // grant admin to BOB on the stream id so he can create the stream
    grantAndAssertSuccess(streamId, BOB, ImmutableSet.of(Action.ADMIN));

    // switch to bob
    SecurityRequestContext.setUserId(BOB.getName());

    // try to create the same stream as bob
    // this will not fail since stream create is idempotent
    getStreamManager(streamId).createStream();

    // verify that alice and bob privilege do not change
    assertAllAccess(ALICE, streamId);
    getAuthorizer().enforce(streamId, BOB, Action.ADMIN);
    try {
      getAuthorizer().enforce(streamId, BOB, EnumSet.of(Action.READ, Action.WRITE, Action.EXECUTE));
    } catch (UnauthorizedException e) {
      // expected
    }

    // set user id back to ALICE so we can delete the namespace and the stream in the namespace
    SecurityRequestContext.setUserId(ALICE.getName());
  }

  @Test
  @Category(SlowTests.class)
  public void testApps() throws Exception {
    try {
      deployApplication(NamespaceId.DEFAULT, DummyApp.class);
      Assert.fail("App deployment should fail because alice does not have ADMIN privilege on the application");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    ApplicationId dummyAppId = AUTH_NAMESPACE.app(DummyApp.class.getSimpleName());
    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(dummyAppId, EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(DummyApp.class.getSimpleName(), "1.0-SNAPSHOT"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset("whom"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.stream("who"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.datasetType(KeyValueTable.class.getName()), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, DummyApp.class);
    // Bob should not have any privileges on Alice's app
    Assert.assertTrue("Bob should not have any privileges on alice's app", authorizer.listPrivileges(BOB).isEmpty());
    // update should succeed because alice has admin privileges on the app
    appManager.update(new AppRequest(new ArtifactSummary(DummyApp.class.getSimpleName(), "1.0-SNAPSHOT")));
    // Update should fail for Bob
    SecurityRequestContext.setUserId(BOB.getName());
    try {
      appManager.update(new AppRequest(new ArtifactSummary(DummyApp.class.getSimpleName(), "1.0-SNAPSHOT")));
      Assert.fail("App update should have failed because Bob does not have admin privileges on the app.");
    } catch (Exception expected) {
      // expected
    }
    // grant READ and WRITE to Bob
    grantAndAssertSuccess(dummyAppId, BOB, ImmutableSet.of(Action.READ, Action.WRITE));
    // delete should fail
    try {
      appManager.delete();
    } catch (Exception expected) {
      // expected
    }
    // grant ADMIN to Bob. Now delete should succeed
    grantAndAssertSuccess(dummyAppId, BOB, ImmutableSet.of(Action.ADMIN));
    // deletion should succeed since BOB has privileges on the app
    appManager.delete();

    // Should still have the privilege for the app since we no longer revoke privileges after deletion of an entity
    Assert.assertTrue(!getAuthorizer().isVisible(Collections.singleton(dummyAppId), BOB).isEmpty());

    // bob should still have privileges granted to him
    Assert.assertEquals(3, authorizer.listPrivileges(BOB).size());
    // switch back to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // Deploy a couple of apps in the namespace
    // Deploy dummy app should be successful since we already pre-grant the required privileges
    deployApplication(AUTH_NAMESPACE, DummyApp.class);

    Map<EntityId, Set<Action>> anotherAppNeededPrivilege = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(AUTH_NAMESPACE.app(AllProgramsApp.NAME), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(AllProgramsApp.class.getSimpleName(), "1.0-SNAPSHOT"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset(AllProgramsApp.DATASET_NAME), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset(AllProgramsApp.DATASET_NAME2), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset(AllProgramsApp.DATASET_NAME3), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset(AllProgramsApp.DS_WITH_SCHEMA_NAME), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.stream(AllProgramsApp.STREAM_NAME), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.datasetType(ObjectMappedTable.class.getName()), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, anotherAppNeededPrivilege);

    deployApplication(AUTH_NAMESPACE, AllProgramsApp.class);

    // Switch to BOB since he does not have any privilege
    SecurityRequestContext.setUserId(BOB.getName());
    // deleting all apps should fail because bob does not have admin privileges on the apps and the namespace
    try {
      deleteAllApplications(AUTH_NAMESPACE);
      Assert.fail("Deleting all applications in the namespace should have failed because bob does not have ADMIN " +
                    "privilege on the workflow app.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // Switch to ALICE, deletion should be successful since ALICE has ADMIN privileges
    SecurityRequestContext.setUserId(ALICE.getName());
    deleteAllApplications(AUTH_NAMESPACE);
  }

  @Test
  public void testArtifacts() throws Exception {
    String appArtifactName = "app-artifact";
    String appArtifactVersion = "1.1.1";
    try {
      ArtifactId defaultNsArtifact = NamespaceId.DEFAULT.artifact(appArtifactName, appArtifactVersion);
      addAppArtifact(defaultNsArtifact, ConfigTestApp.class);
      Assert.fail("Should not be able to add an app artifact to the default namespace because alice does not have " +
                    "admin privileges on the artifact.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    String pluginArtifactName = "plugin-artifact";
    String pluginArtifactVersion = "1.2.3";
    try {
      ArtifactId defaultNsArtifact = NamespaceId.DEFAULT.artifact(pluginArtifactName, pluginArtifactVersion);
      addAppArtifact(defaultNsArtifact, ToStringPlugin.class);
      Assert.fail("Should not be able to add a plugin artifact to the default namespace because alice does not have " +
                    "admin privileges on the artifact.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // create a new namespace
    createAuthNamespace();
    ArtifactId appArtifactId = AUTH_NAMESPACE.artifact(appArtifactName, appArtifactVersion);
    grantAndAssertSuccess(appArtifactId, ALICE, EnumSet.of(Action.ADMIN));
    cleanUpEntities.add(appArtifactId);
    ArtifactManager appArtifactManager = addAppArtifact(appArtifactId, ConfigTestApp.class);
    ArtifactId pluginArtifactId = AUTH_NAMESPACE.artifact(pluginArtifactName, pluginArtifactVersion);
    grantAndAssertSuccess(pluginArtifactId, ALICE, EnumSet.of(Action.ADMIN));
    cleanUpEntities.add(pluginArtifactId);
    ArtifactManager pluginArtifactManager = addPluginArtifact(pluginArtifactId, appArtifactId, ToStringPlugin.class);
    // Bob should not be able to delete or write properties to artifacts since he does not have ADMIN permission on
    // the artifacts
    SecurityRequestContext.setUserId(BOB.getName());
    try {
      appArtifactManager.writeProperties(ImmutableMap.of("authorized", "no"));
      Assert.fail("Writing properties to artifact should have failed because Bob does not have admin privileges on " +
                    "the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      appArtifactManager.delete();
      Assert.fail("Deleting artifact should have failed because Bob does not have admin privileges on the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      pluginArtifactManager.writeProperties(ImmutableMap.of("authorized", "no"));
      Assert.fail("Writing properties to artifact should have failed because Bob does not have admin privileges on " +
                    "the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      pluginArtifactManager.removeProperties();
      Assert.fail("Removing properties to artifact should have failed because Bob does not have admin privileges on " +
                    "the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      pluginArtifactManager.delete();
      Assert.fail("Deleting artifact should have failed because Bob does not have admin privileges on the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // alice should be permitted to update properties/delete artifact
    SecurityRequestContext.setUserId(ALICE.getName());
    appArtifactManager.writeProperties(ImmutableMap.of("authorized", "yes"));
    appArtifactManager.removeProperties();
    appArtifactManager.delete();
    pluginArtifactManager.delete();
  }

  @Test
  public void testPrograms() throws Exception {
    createAuthNamespace();
    grantAndAssertSuccess(AUTH_NAMESPACE.app(DummyApp.class.getSimpleName()), ALICE, EnumSet.of(Action.ADMIN));
    ApplicationId dummyAppId = AUTH_NAMESPACE.app(DummyApp.class.getSimpleName());
    final ProgramId serviceId = dummyAppId.service(DummyApp.Greeting.SERVICE_NAME);
    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(dummyAppId, EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(DummyApp.class.getSimpleName(), "1.0-SNAPSHOT"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset("whom"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.stream("who"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.datasetType(KeyValueTable.class.getName()), EnumSet.of(Action.ADMIN))
      .put(serviceId, EnumSet.of(Action.EXECUTE, Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    final ApplicationManager dummyAppManager = deployApplication(AUTH_NAMESPACE, DummyApp.class);

    // alice should be able to start and stop programs in the app she deployed since she has execute privilege
    dummyAppManager.startProgram(serviceId.toId());

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return dummyAppManager.isRunning(serviceId.toId());
      }
    }, 5, TimeUnit.SECONDS);
    ServiceManager greetingService = dummyAppManager.getServiceManager(serviceId.getProgram());
    // alice should be able to set instances for the program
    greetingService.setInstances(2);
    Assert.assertEquals(2, greetingService.getProvisionedInstances());
    // alice should also be able to save runtime arguments for all future runs of the program
    Map<String, String> args = ImmutableMap.of("key", "value");
    greetingService.setRuntimeArgs(args);
    // Alice should be able to get runtime arguments as she has ADMIN on it
    Assert.assertEquals(args, greetingService.getRuntimeArgs());
    dummyAppManager.stopProgram(serviceId.toId());
    Tasks.waitFor(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return dummyAppManager.isRunning(serviceId.toId());
      }
    }, 5, TimeUnit.SECONDS);
    // Bob should not be able to start programs in dummy app because he does not have privileges on it
    SecurityRequestContext.setUserId(BOB.getName());
    try {
      dummyAppManager.startProgram(serviceId.toId());
      Assert.fail("Bob should not be able to start the service because he does not have execute privileges on it.");
    } catch (RuntimeException expected) {
      //noinspection ThrowableResultOfMethodCallIgnored
      Assert.assertTrue(Throwables.getRootCause(expected) instanceof UnauthorizedException);
    }
    try {
      dummyAppManager.getInfo();
      Assert.fail("Bob should not be able to read the app info with out privileges");
    } catch (Exception expected) {
      // expected
    }

    // TODO: CDAP-5452 can't verify running programs in this case, because DefaultApplicationManager maintains an
    // in-memory map of running processes that does not use ApplicationLifecycleService to get the runtime status.
    // So no matter if the start/stop call succeeds or fails, it updates its running state in the in-memory map.
    // Also have to switch back to being alice, start the program, and then stop it as Bob because otherwise AppManager
    // doesn't send the request to the app fabric service, but just makes decisions based on an in-memory
    // ConcurrentHashMap.
    // Also add a test for stopping with unauthorized user after the above bug is fixed

    // setting instances should fail because Bob does not have admin privileges on the program
    try {
      greetingService.setInstances(3);
      Assert.fail("Setting instances should have failed because bob does not have admin privileges on the service.");
    } catch (RuntimeException expected) {
      //noinspection ThrowableResultOfMethodCallIgnored
      Assert.assertTrue(Throwables.getRootCause(expected) instanceof UnauthorizedException);
    }
    try {
      greetingService.setRuntimeArgs(args);
      Assert.fail("Setting runtime arguments should have failed because bob does not have admin privileges on the " +
                    "service");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      greetingService.getRuntimeArgs();
      Assert.fail("Getting runtime arguments should have failed because bob does not have one of READ, WRITE, ADMIN " +
                    "privileges on the service");
    } catch (UnauthorizedException expected) {
      // expected
    }

    SecurityRequestContext.setUserId(ALICE.getName());
    dummyAppManager.delete();
  }

  @Test
  public void testCrossNSFlowlet() throws Exception {
    createAuthNamespace();
    ApplicationId appId = AUTH_NAMESPACE.app(CrossNsDatasetAccessApp.APP_NAME);
    StreamId streamId = AUTH_NAMESPACE.stream(CrossNsDatasetAccessApp.STREAM_NAME);
    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(appId, EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(CrossNsDatasetAccessApp.class.getSimpleName(), "1.0-SNAPSHOT"),
           EnumSet.of(Action.ADMIN))
      .put(streamId, EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    ProgramId programId = appId.flow(CrossNsDatasetAccessApp.FLOW_NAME);
    cleanUpEntities.add(programId);
    // grant bob execute on program and READ/WRITE on stream
    grantAndAssertSuccess(programId, BOB, EnumSet.of(Action.EXECUTE));
    grantAndAssertSuccess(streamId, BOB, EnumSet.of(Action.WRITE, Action.READ));

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, CrossNsDatasetAccessApp.class);

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());

    // Send data to stream as BOB this ensures that BOB can write to a stream in auth namespace
    StreamManager streamManager = getStreamManager(AUTH_NAMESPACE.stream(CrossNsDatasetAccessApp.STREAM_NAME));
    for (int i = 0; i < 10; i++) {
      streamManager.send(String.valueOf(i).getBytes());
    }

    // switch to back to ALICE
    SecurityRequestContext.setUserId(ALICE.getName());

    final FlowManager flowManager = appManager.getFlowManager(CrossNsDatasetAccessApp.FLOW_NAME);

    testSystemDatasetAccessFromFlowlet(flowManager);
    testCrossNSDatasetAccessFromFlowlet(flowManager);
  }

  private void testSystemDatasetAccessFromFlowlet(final FlowManager flowManager) throws Exception {
    addDatasetInstance(NamespaceId.SYSTEM.dataset("store"), "keyValueTable");

    // give bob write permission on the dataset
    grantAndAssertSuccess(NamespaceId.SYSTEM.dataset("store"), BOB, EnumSet.of(Action.WRITE));

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());

    Map<String, String> args = ImmutableMap.of(
      CrossNsDatasetAccessApp.OUTPUT_DATASET_NS, NamespaceId.SYSTEM.getNamespace(),
      CrossNsDatasetAccessApp.OUTPUT_DATASET_NAME, "store"
    );

    // But trying to run a flow as BOB will fail since this flow writes to a dataset in system namespace
    flowManager.start(args);
    // wait for flow to be running
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return flowManager.isRunning();
      }
    }, 120, TimeUnit.SECONDS);

    // The above will be a runtime failure after the flow start since it will not be able to use the dataset in the
    // system namespace. Since the failure will lead to no metrics being emitted we cannot actually check it tried
    // processing or not. So stop the flow and check that the output dataset is empty
    flowManager.stop();

    assertDatasetIsEmpty(NamespaceId.SYSTEM, "store");

    // switch to back to ALICE
    SecurityRequestContext.setUserId(ALICE.getName());

    // cleanup
    deleteDatasetInstance(NamespaceId.SYSTEM.dataset("store"));
  }

  private void testCrossNSDatasetAccessFromFlowlet(final FlowManager flowManager) throws Exception {
    NamespaceMeta outputDatasetNS = new NamespaceMeta.Builder().setName("outputNS").build();
    NamespaceId outputDatasetNSId = outputDatasetNS.getNamespaceId();
    DatasetId datasetId = outputDatasetNSId.dataset("store");
    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(outputDatasetNSId, EnumSet.of(Action.ADMIN))
      .put(datasetId, EnumSet.of(Action.ADMIN, Action.READ))
      .put(outputDatasetNSId.datasetType("keyValueTable"), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges
    );
    getNamespaceAdmin().create(outputDatasetNS);
    addDatasetInstance(datasetId, "keyValueTable");

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());

    Map<String, String> args = ImmutableMap.of(
      CrossNsDatasetAccessApp.OUTPUT_DATASET_NS, outputDatasetNS.getNamespaceId().getNamespace(),
      CrossNsDatasetAccessApp.OUTPUT_DATASET_NAME, "store"
    );

    // But trying to run a flow as BOB will fail since this flow writes to a dataset in another namespace in which
    // is not accessible to BOB.
    flowManager.start(args);
    // wait for flow to be running
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return flowManager.isRunning();
      }
    }, 120, TimeUnit.SECONDS);

    // The above will be a runtime failure after the flow start since it will not be able to use the dataset in the
    // another namespace. Since the failure will lead to no metrics being emitted we cannot actually check it tried
    // processing or not. So stop the flow and check that the output dataset is empty
    flowManager.stop();
    SecurityRequestContext.setUserId(ALICE.getName());

    assertDatasetIsEmpty(outputDatasetNS.getNamespaceId(), "store");

    // Give BOB permission to write to the dataset in another namespace
    grantAndAssertSuccess(datasetId, BOB, EnumSet.of(Action.WRITE));

    // switch back to BOB to run flow again
    SecurityRequestContext.setUserId(BOB.getName());

    // running the flow now should pass and write data in another namespace successfully
    flowManager.start(args);
    flowManager.getFlowletMetrics("saver").waitForProcessed(10, 30, TimeUnit.SECONDS);
    flowManager.stop();

    // switch back to alice and verify the data its fine now to verify the run record here because if the flow failed
    // to write we will not see any data
    SecurityRequestContext.setUserId(ALICE.getName());

    DataSetManager<KeyValueTable> dataSetManager = getDataset(outputDatasetNS.getNamespaceId().dataset("store"));
    KeyValueTable results = dataSetManager.get();

    for (int i = 0; i < 10; i++) {
      byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
      Assert.assertArrayEquals(key, results.read(key));
    }
    getNamespaceAdmin().delete(outputDatasetNS.getNamespaceId());
  }

  @Test
  public void testCrossNSMapReduce() throws Exception {
    createAuthNamespace();
    ApplicationId appId = AUTH_NAMESPACE.app(DatasetCrossNSAccessWithMAPApp.class.getSimpleName());

    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(appId, EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(DatasetCrossNSAccessWithMAPApp.class.getSimpleName(), "1.0-SNAPSHOT"),
           EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    ProgramId programId = appId.program(ProgramType.MAPREDUCE, DatasetCrossNSAccessWithMAPApp.MAPREDUCE_PROGRAM);
    // bob will be executing the program
    grantAndAssertSuccess(programId, BOB, EnumSet.of(Action.EXECUTE));
    cleanUpEntities.add(programId);

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, DatasetCrossNSAccessWithMAPApp.class);

    MapReduceManager mrManager = appManager.getMapReduceManager(DatasetCrossNSAccessWithMAPApp.MAPREDUCE_PROGRAM);

    testCrossNSSystemDatasetAccessWithAuthMapReduce(mrManager);
    testCrossNSDatasetAccessWithAuthMapReduce(mrManager);
  }

  private void testCrossNSSystemDatasetAccessWithAuthMapReduce(MapReduceManager mrManager) throws Exception {
    addDatasetInstance(NamespaceId.SYSTEM.dataset("table1"), "keyValueTable").create();
    addDatasetInstance(NamespaceId.SYSTEM.dataset("table2"), "keyValueTable").create();
    NamespaceMeta otherNS = new NamespaceMeta.Builder().setName("otherNS").build();
    NamespaceId otherNsId = otherNS.getNamespaceId();
    DatasetId datasetId = otherNsId.dataset("otherTable");
    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(otherNsId, EnumSet.of(Action.ADMIN))
      .put(datasetId, EnumSet.of(Action.ADMIN))
      .put(otherNsId.datasetType("keyValueTable"), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    getNamespaceAdmin().create(otherNS);
    addDatasetInstance(datasetId, "keyValueTable").create();
    addDummyData(NamespaceId.SYSTEM, "table1");

    // first test that reading system namespace fails with valid table as output
    Map<String, String> argsForMR = ImmutableMap.of(
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NS, NamespaceId.SYSTEM.getNamespace(),
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NAME, "table1",
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NS, otherNS.getNamespaceId().getNamespace(),
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NAME, "otherTable");

    // give privilege to BOB on all the datasets
    grantAndAssertSuccess(NamespaceId.SYSTEM.dataset("table1"), BOB, EnumSet.of(Action.READ));
    grantAndAssertSuccess(NamespaceId.SYSTEM.dataset("table2"), BOB, EnumSet.of(Action.WRITE));
    grantAndAssertSuccess(otherNS.getNamespaceId().dataset("otherTable"), BOB, ALL_ACTIONS);

    // Switch to BOB and run the  mapreduce job. The job will fail at the runtime since BOB is trying to read from
    // system namespace
    SecurityRequestContext.setUserId(BOB.getName());
    assertProgramFailure(argsForMR, mrManager);
    assertDatasetIsEmpty(otherNS.getNamespaceId(), "otherTable");

    // now try reading a table from valid namespace and writing to system namespace
    argsForMR = ImmutableMap.of(
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NS, otherNS.getName(),
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NAME, "otherTable",
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NS, NamespaceId.SYSTEM.getNamespace(),
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NAME, "table2");

    addDummyData(otherNS.getNamespaceId(), "otherTable");

    // verify that the program fails
    assertProgramFailure(argsForMR, mrManager);
    assertDatasetIsEmpty(NamespaceId.SYSTEM, "table2");

    // switch to back to ALICE
    SecurityRequestContext.setUserId(ALICE.getName());

    // cleanup
    deleteDatasetInstance(NamespaceId.SYSTEM.dataset("table1"));
    deleteDatasetInstance(NamespaceId.SYSTEM.dataset("table2"));
    getNamespaceAdmin().delete(otherNS.getNamespaceId());
  }

  private void testCrossNSDatasetAccessWithAuthMapReduce(MapReduceManager mrManager) throws Exception {
    NamespaceMeta inputDatasetNS = new NamespaceMeta.Builder().setName("inputNS").build();
    NamespaceId inputDatasetNSId = inputDatasetNS.getNamespaceId();
    NamespaceMeta outputDatasetNS = new NamespaceMeta.Builder().setName("outputNS").build();
    NamespaceId outputDatasetNSId = outputDatasetNS.getNamespaceId();
    DatasetId table1Id = inputDatasetNSId.dataset("table1");
    DatasetId table2Id = outputDatasetNSId.dataset("table2");

    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(inputDatasetNSId, EnumSet.of(Action.ADMIN))
      .put(outputDatasetNSId, EnumSet.of(Action.ADMIN))
      // We need to write some data into table1
      .put(table1Id, EnumSet.of(Action.ADMIN, Action.WRITE))
      // Need to read data from table2
      .put(table2Id, EnumSet.of(Action.ADMIN, Action.READ))
      .put(inputDatasetNSId.datasetType("keyValueTable"), EnumSet.of(Action.ADMIN))
      .put(outputDatasetNSId.datasetType("keyValueTable"), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    getNamespaceAdmin().create(inputDatasetNS);
    getNamespaceAdmin().create(outputDatasetNS);
    addDatasetInstance(table1Id, "keyValueTable").create();
    addDatasetInstance(table2Id, "keyValueTable").create();

    addDummyData(inputDatasetNSId, "table1");

    Map<String, String> argsForMR = ImmutableMap.of(
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NS, inputDatasetNS.getNamespaceId().getNamespace(),
      DatasetCrossNSAccessWithMAPApp.INPUT_DATASET_NAME, "table1",
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NS, outputDatasetNS.getNamespaceId().getNamespace(),
      DatasetCrossNSAccessWithMAPApp.OUTPUT_DATASET_NAME, "table2");

    // Switch to BOB and run the  mapreduce job. The job will fail at the runtime since BOB does not have permission
    // on the input and output datasets in another namespaces.
    SecurityRequestContext.setUserId(BOB.getName());
    assertProgramFailure(argsForMR, mrManager);

    // Switch back to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // Verify nothing write to the output dataset
    assertDatasetIsEmpty(outputDatasetNS.getNamespaceId(), "table2");

    // give privilege to BOB on the input dataset
    grantAndAssertSuccess(inputDatasetNS.getNamespaceId().dataset("table1"), BOB, EnumSet.of(Action.READ));

    // switch back to bob and try running again. this will still fail since bob does not have access on the output
    // dataset
    SecurityRequestContext.setUserId(BOB.getName());
    assertProgramFailure(argsForMR, mrManager);

    // Switch back to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // Verify nothing write to the output dataset
    assertDatasetIsEmpty(outputDatasetNS.getNamespaceId(), "table2");

    // give privilege to BOB on the output dataset
    grantAndAssertSuccess(outputDatasetNS.getNamespaceId().dataset("table2"), BOB, EnumSet.of(Action.WRITE));

    // switch back to BOB and run MR again. this should work
    SecurityRequestContext.setUserId(BOB.getName());
    mrManager.start(argsForMR);
    mrManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    // Verify results as alice
    SecurityRequestContext.setUserId(ALICE.getName());
    verifyDummyData(outputDatasetNS.getNamespaceId(), "table2");
    getNamespaceAdmin().delete(inputDatasetNS.getNamespaceId());
    getNamespaceAdmin().delete(outputDatasetNS.getNamespaceId());
  }

  @Test
  public void testCrossNSSpark() throws Exception {
    createAuthNamespace();
    ApplicationId appId = AUTH_NAMESPACE.app(TestSparkCrossNSDatasetApp.APP_NAME);

    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(appId, EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(TestSparkCrossNSDatasetApp.class.getSimpleName(), "1.0-SNAPSHOT"),
           EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset(TestSparkCrossNSDatasetApp.DEFAULT_OUTPUT_DATASET), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.datasetType(KeyValueTable.class.getName()), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    ProgramId programId = appId.spark(TestSparkCrossNSDatasetApp.SPARK_PROGRAM_NAME);
    // bob will be executing the program
    grantAndAssertSuccess(programId, BOB, EnumSet.of(Action.EXECUTE));
    cleanUpEntities.add(programId);

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, TestSparkCrossNSDatasetApp.class);
    SparkManager sparkManager = appManager.getSparkManager(TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram
                                                             .class.getSimpleName());

    testCrossNSSystemDatasetAccessWithAuthSpark(sparkManager);
    testCrossNSDatasetAccessWithAuthSpark(sparkManager);
  }

  @Test
  public void testScheduleAuth() throws Exception {
    createAuthNamespace();
    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(AUTH_NAMESPACE.app(AppWithSchedule.class.getSimpleName()), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(AppWithSchedule.class.getSimpleName(), "1.0-SNAPSHOT"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset(AppWithSchedule.INPUT_NAME), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset(AppWithSchedule.OUTPUT_NAME), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.datasetType(ObjectStore.class.getName()), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE, AppWithSchedule.class);
    ProgramId workflowID = new ProgramId(AUTH_NAMESPACE.getNamespace(), AppWithSchedule.class.getSimpleName(),
                                         ProgramType.WORKFLOW, AppWithSchedule.SampleWorkflow.class.getSimpleName());
    cleanUpEntities.add(workflowID);

    final WorkflowManager workflowManager =
      appManager.getWorkflowManager(AppWithSchedule.SampleWorkflow.class.getSimpleName());
    ScheduleManager scheduleManager = workflowManager.getSchedule(AppWithSchedule.SCHEDULE_NAME);

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());
    // try to resume schedule as BOB. It should fail since BOB does not have execute privileges on the programs
    try {
      scheduleManager.resume();
      Assert.fail("Resuming schedule should have failed since BOB does not have EXECUTE on the program");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    // bob should also not be able see the status of the schedule
    try {
      scheduleManager.status(HttpURLConnection.HTTP_FORBIDDEN);
      Assert.fail("Getting schedule status should have failed since BOB does not have READ on the program");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    // switch to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // give BOB READ permission in the workflow
    grantAndAssertSuccess(workflowID, BOB, EnumSet.of(Action.READ));

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());
    // try to resume schedule as BOB. It should fail since BOB has READ but not EXECUTE on the workflow
    try {
      scheduleManager.resume();
      Assert.fail("Resuming schedule should have failed since BOB does not have EXECUTE on the program");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    // but BOB should be able to get schedule status now
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED.name(), scheduleManager.status(HttpURLConnection.HTTP_OK));

    // switch to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // give BOB EXECUTE permission in the workflow
    grantAndAssertSuccess(workflowID, BOB, EnumSet.of(Action.EXECUTE));

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());
    // try to resume the schedule. This should pass and workflow should run
    scheduleManager.resume();
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED.name(), scheduleManager.status(HttpURLConnection.HTTP_OK));

    // todo: remove grant after https://issues.cask.co/browse/CDAP-12147 is fixed
    grantAndAssertSuccess(workflowID, new Principal(UserGroupInformation.getLoginUser().getShortUserName(),
                                                    Principal.PrincipalType.USER),
                          EnumSet.of(Action.EXECUTE));
    
    // wait for workflow to start
    workflowManager.waitForStatus(true);

    // suspend the schedule so that it does not start running again
    scheduleManager.suspend();
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED.name(), scheduleManager.status(HttpURLConnection.HTTP_OK));

    // stop all the runs of the workflow so that the current namespace can be deleted after the test
    workflowManager.stop();
    workflowManager.waitForStatus(false, 5, 10);
    // switch to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
  }

  private void testCrossNSSystemDatasetAccessWithAuthSpark(SparkManager sparkManager) throws Exception {
    addDatasetInstance(NamespaceId.SYSTEM.dataset("table1"), "keyValueTable").create();
    addDatasetInstance(NamespaceId.SYSTEM.dataset("table2"), "keyValueTable").create();
    NamespaceMeta otherNS = new NamespaceMeta.Builder().setName("otherNS").build();
    NamespaceId otherNSId = otherNS.getNamespaceId();
    DatasetId otherTableId = otherNSId.dataset("otherTable");

    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(otherNSId, EnumSet.of(Action.ADMIN))
      .put(otherTableId, EnumSet.of(Action.ADMIN))
      .put(otherNSId.datasetType("keyValueTable"), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    getNamespaceAdmin().create(otherNS);
    addDatasetInstance(otherTableId, "keyValueTable").create();
    addDummyData(NamespaceId.SYSTEM, "table1");

    // give privilege to BOB on all the datasets
    grantAndAssertSuccess(NamespaceId.SYSTEM.dataset("table1"), BOB, EnumSet.of(Action.READ));
    grantAndAssertSuccess(NamespaceId.SYSTEM.dataset("table2"), BOB, EnumSet.of(Action.WRITE));
    grantAndAssertSuccess(otherNS.getNamespaceId().dataset("otherTable"), BOB, ALL_ACTIONS);

    // Switch to Bob and run the spark program. this will fail because bob is trying to read from a system dataset
    SecurityRequestContext.setUserId(BOB.getName());
    Map<String, String> args = ImmutableMap.of(
      TestSparkCrossNSDatasetApp.INPUT_DATASET_NAMESPACE,
      NamespaceId.SYSTEM.getNamespace(),
      TestSparkCrossNSDatasetApp.INPUT_DATASET_NAME, "table1",
      TestSparkCrossNSDatasetApp.OUTPUT_DATASET_NAMESPACE,
      otherNS.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.OUTPUT_DATASET_NAME, "otherTable"
    );

    assertProgramFailure(args, sparkManager);
    assertDatasetIsEmpty(otherNS.getNamespaceId(), "otherTable");

    // try running spark job with valid input namespace but writing to system namespace this should fail too
    args = ImmutableMap.of(
      TestSparkCrossNSDatasetApp.INPUT_DATASET_NAMESPACE,
      otherNS.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.INPUT_DATASET_NAME, "otherTable",
      TestSparkCrossNSDatasetApp.OUTPUT_DATASET_NAMESPACE,
      NamespaceId.SYSTEM.getNamespace(),
      TestSparkCrossNSDatasetApp.OUTPUT_DATASET_NAME, "table2"
    );

    addDummyData(otherNS.getNamespaceId(), "otherTable");

    assertProgramFailure(args, sparkManager);
    assertDatasetIsEmpty(NamespaceId.SYSTEM, "table2");

    // switch to back to ALICE
    SecurityRequestContext.setUserId(ALICE.getName());

    // cleanup
    deleteDatasetInstance(NamespaceId.SYSTEM.dataset("table1"));
    deleteDatasetInstance(NamespaceId.SYSTEM.dataset("table2"));
    getNamespaceAdmin().delete(otherNS.getNamespaceId());
  }

  private void testCrossNSDatasetAccessWithAuthSpark(SparkManager sparkManager) throws Exception {
    NamespaceMeta inputDatasetNSMeta = new NamespaceMeta.Builder().setName("inputDatasetNS").build();
    NamespaceMeta outputDatasetNSMeta = new NamespaceMeta.Builder().setName("outputDatasetNS").build();
    NamespaceId inputDatasetNSMetaId = inputDatasetNSMeta.getNamespaceId();
    DatasetId inputTableId = inputDatasetNSMetaId.dataset("input");
    NamespaceId outputDatasetNSMetaId = outputDatasetNSMeta.getNamespaceId();
    DatasetId outputTableId = outputDatasetNSMetaId.dataset("output");

    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(inputDatasetNSMetaId, EnumSet.of(Action.ADMIN))
      .put(outputDatasetNSMetaId, EnumSet.of(Action.ADMIN))
      .put(inputTableId, EnumSet.of(Action.ADMIN, Action.WRITE))
      .put(inputDatasetNSMetaId.datasetType("keyValueTable"), EnumSet.of(Action.ADMIN))
      .put(outputTableId, EnumSet.of(Action.ADMIN, Action.READ))
      .put(outputDatasetNSMetaId.datasetType("keyValueTable"), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    getNamespaceAdmin().create(inputDatasetNSMeta);
    getNamespaceAdmin().create(outputDatasetNSMeta);
    addDatasetInstance(inputTableId, "keyValueTable").create();
    addDatasetInstance(outputTableId, "keyValueTable").create();
    // write sample stuff in input dataset
    addDummyData(inputDatasetNSMeta.getNamespaceId(), "input");

    // Switch to Bob and run the spark program. this will fail because bob does not have access to either input or
    // output dataset
    SecurityRequestContext.setUserId(BOB.getName());
    Map<String, String> args = ImmutableMap.of(
      TestSparkCrossNSDatasetApp.INPUT_DATASET_NAMESPACE,
      inputDatasetNSMeta.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.INPUT_DATASET_NAME, "input",
      TestSparkCrossNSDatasetApp.OUTPUT_DATASET_NAMESPACE,
      outputDatasetNSMeta.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.OUTPUT_DATASET_NAME, "output"
    );

    assertProgramFailure(args, sparkManager);

    SecurityRequestContext.setUserId(ALICE.getName());
    // Verify nothing write to the output dataset
    assertDatasetIsEmpty(outputDatasetNSMeta.getNamespaceId(), "output");

    // give privilege to BOB on the input dataset
    grantAndAssertSuccess(inputDatasetNSMeta.getNamespaceId().dataset("input"), BOB, EnumSet.of(Action.READ));

    // switch back to bob and try running again. this will still fail since bob does not have access on the output
    // dataset
    SecurityRequestContext.setUserId(BOB.getName());
    assertProgramFailure(args, sparkManager);

    // Switch back to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // Verify nothing write to the output dataset
    assertDatasetIsEmpty(outputDatasetNSMeta.getNamespaceId(), "output");

    // give privilege to BOB on the output dataset
    grantAndAssertSuccess(outputDatasetNSMeta.getNamespaceId().dataset("output"), BOB, EnumSet.of(Action.WRITE));

    // switch back to BOB and run spark again. this should work
    SecurityRequestContext.setUserId(BOB.getName());

    sparkManager.start(args);
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 120, TimeUnit.SECONDS);

    // Verify the results as alice
    SecurityRequestContext.setUserId(ALICE.getName());
    verifyDummyData(outputDatasetNSMeta.getNamespaceId(), "output");
    getNamespaceAdmin().delete(inputDatasetNSMeta.getNamespaceId());
    getNamespaceAdmin().delete(outputDatasetNSMeta.getNamespaceId());
  }

  @Test
  public void testAddDropPartitions() throws Exception {
    createAuthNamespace();
    ApplicationId appId = AUTH_NAMESPACE.app(PartitionTestApp.class.getSimpleName());
    DatasetId datasetId = AUTH_NAMESPACE.dataset(PartitionTestApp.PFS_NAME);

    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(appId, EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(PartitionTestApp.class.getSimpleName(), "1.0-SNAPSHOT"), EnumSet.of(Action.ADMIN))
      .put(datasetId, EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.datasetType(PartitionedFileSet.class.getName()), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);

    ProgramId programId = appId.program(ProgramType.SERVICE, PartitionTestApp.PFS_SERVICE_NAME);
    grantAndAssertSuccess(programId, BOB, EnumSet.of(Action.EXECUTE));
    cleanUpEntities.add(programId);
    grantAndAssertSuccess(datasetId, BOB, EnumSet.of(Action.READ));
    cleanUpEntities.add(datasetId);

    ApplicationManager appMgr = deployApplication(AUTH_NAMESPACE, PartitionTestApp.class);
    SecurityRequestContext.setUserId(BOB.getName());
    String partition = "p1";
    String subPartition = "1";
    String text = "some random text for pfs";
    ServiceManager pfsService = appMgr.getServiceManager(PartitionTestApp.PFS_SERVICE_NAME);

    pfsService.start();
    pfsService.waitForRun(ProgramRunStatus.RUNNING, 1, TimeUnit.MINUTES);
    URL pfsURL = pfsService.getServiceURL();
    String apiPath = String.format("partitions/%s/subpartitions/%s", partition, subPartition);
    URL url = new URL(pfsURL, apiPath);
    HttpRequest request;
    HttpResponse response;
    try {
      request = HttpRequest.post(url).withBody(text).build();
      response = HttpRequests.execute(request);
      // should fail because bob does not have write privileges on the dataset
      Assert.assertEquals(500, response.getResponseCode());
    } finally {
      pfsService.stop();
      pfsService.waitForRun(ProgramRunStatus.KILLED, 1, TimeUnit.MINUTES);
    }
    // grant read and write on dataset and restart
    grantAndAssertSuccess(datasetId, BOB, EnumSet.of(Action.WRITE, Action.READ));
    pfsService.start();
    pfsService.waitForRun(ProgramRunStatus.RUNNING, 1, TimeUnit.MINUTES);
    pfsURL = pfsService.getServiceURL();
    url = new URL(pfsURL, apiPath);
    try  {
      request = HttpRequest.post(url).withBody(text).build();
      response = HttpRequests.execute(request);
      // should succeed now because bob was granted write privileges on the dataset
      Assert.assertEquals(200, response.getResponseCode());
      // make sure that the partition was added
      request = HttpRequest.get(url).build();
      response = HttpRequests.execute(request);
      Assert.assertEquals(200, response.getResponseCode());
      Assert.assertEquals(text, response.getResponseBodyAsString());
      // drop the partition
      request = HttpRequest.delete(url).build();
      response = HttpRequests.execute(request);
      Assert.assertEquals(200, response.getResponseCode());
    } finally {
      pfsService.stop();
      pfsService.waitForRuns(ProgramRunStatus.KILLED, 2, 1, TimeUnit.MINUTES);
      SecurityRequestContext.setUserId(ALICE.getName());
    }
  }

  @After
  @Override
  public void afterTest() throws Exception {
    Authorizer authorizer = getAuthorizer();

    grantAndAssertSuccess(AUTH_NAMESPACE, SecurityRequestContext.toPrincipal(), EnumSet.of(Action.ADMIN));
    // clean up. remove the namespace.
    getNamespaceAdmin().delete(AUTH_NAMESPACE);
    Assert.assertFalse(getNamespaceAdmin().exists(AUTH_NAMESPACE));
    revokeAndAssertSuccess(AUTH_NAMESPACE);
    for (EntityId entityId : cleanUpEntities) {
      revokeAndAssertSuccess(entityId);
    }
    Assert.assertEquals(Collections.emptySet(), authorizer.listPrivileges(ALICE));
  }

  @AfterClass
  public static void cleanup() throws Exception {
    // we want to execute TestBase's @AfterClass after unsetting userid, because the old userid has been granted ADMIN
    // on default namespace in TestBase so it can clean the namespace.
    SecurityRequestContext.setUserId(oldUser);
    finish();
  }

  private void createAuthNamespace() throws Exception {
    Authorizer authorizer = getAuthorizer();
    grantAndAssertSuccess(AUTH_NAMESPACE, ALICE, ImmutableSet.of(Action.ADMIN));
    getNamespaceAdmin().create(AUTH_NAMESPACE_META);
    Assert.assertEquals(ImmutableSet.of(new Privilege(AUTH_NAMESPACE, Action.ADMIN)), authorizer.listPrivileges(ALICE));
  }

  private void grantAndAssertSuccess(EntityId entityId, Principal principal, Set<Action> actions) throws Exception {
    Authorizer authorizer = getAuthorizer();
    Set<Privilege> existingPrivileges = authorizer.listPrivileges(principal);
    authorizer.grant(entityId, principal, actions);
    ImmutableSet.Builder<Privilege> expectedPrivilegesAfterGrant = ImmutableSet.builder();
    for (Action action : actions) {
      expectedPrivilegesAfterGrant.add(new Privilege(entityId, action));
    }
    Assert.assertEquals(Sets.union(existingPrivileges, expectedPrivilegesAfterGrant.build()),
                        authorizer.listPrivileges(principal));
  }

  private void revokeAndAssertSuccess(final EntityId entityId) throws Exception {
    Authorizer authorizer = getAuthorizer();
    authorizer.revoke(entityId);
    assertNoAccess(entityId);
  }

  private void assertNoAccess(Principal principal, final EntityId entityId) throws Exception {
    Authorizer authorizer = getAuthorizer();
    Predicate<Privilege> entityFilter = new Predicate<Privilege>() {
      @Override
      public boolean apply(Privilege input) {
        return Authorizable.fromEntityId(entityId).equals(input.getAuthorizable());
      }
    };
    Assert.assertTrue(Sets.filter(authorizer.listPrivileges(principal), entityFilter).isEmpty());
  }
  private void assertNoAccess(final EntityId entityId) throws Exception {
    assertNoAccess(ALICE, entityId);
    assertNoAccess(BOB, entityId);
  }

  private void assertDatasetIsEmpty(NamespaceId namespaceId, String datasetName) throws Exception {
    DataSetManager<KeyValueTable> outTableManager = getDataset(namespaceId.dataset(datasetName));
    KeyValueTable outputTable = outTableManager.get();
    try (CloseableIterator<KeyValue<byte[], byte[]>> scanner = outputTable.scan(null, null)) {
      Assert.assertFalse(scanner.hasNext());
    }
  }

  private <T extends ProgramManager> void assertProgramFailure(
    Map<String, String> programArgs, final ProgramManager<T> programManager)
    throws TimeoutException, InterruptedException, ExecutionException {
    programManager.start(programArgs);

    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        // verify program history just have failures
        List<RunRecord> history = programManager.getHistory();
        for (final RunRecord runRecord : history) {
          if (runRecord.getStatus() != ProgramRunStatus.FAILED) {
            return false;
          }
        }
        return !history.isEmpty();
      }
    }, 5, TimeUnit.MINUTES, "Not all program runs have failed status. Expected all run status to be failed");

    Tasks.waitFor(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return programManager.isRunning();
      }
    }, 60, TimeUnit.SECONDS);
  }


  private void assertAllAccess(Principal principal, EntityId... entityIds) throws Exception {
    for (EntityId entityId : entityIds) {
      getAuthorizer().enforce(entityId, principal, EnumSet.allOf(Action.class));
    }
  }

  private void addDummyData(NamespaceId namespaceId, String datasetName) throws Exception {
    DataSetManager<KeyValueTable> tableManager = getDataset(namespaceId.dataset(datasetName));
    KeyValueTable inputTable = tableManager.get();
    inputTable.write("hello", "world");
    tableManager.flush();
  }

  private void verifyDummyData(NamespaceId namespaceId, String datasetName) throws Exception {
    DataSetManager<KeyValueTable> outTableManager = getDataset(namespaceId.dataset(datasetName));
    KeyValueTable outputTable = outTableManager.get();
    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));
  }

  private void setUpPrivilegeToDeployStreamAuthApp() throws Exception {
    Map<EntityId, Set<Action>> neededPrivileges = ImmutableMap.<EntityId, Set<Action>>builder()
      .put(AUTH_NAMESPACE.app(StreamAuthApp.APP), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.artifact(StreamAuthApp.class.getSimpleName(), "1.0-SNAPSHOT"), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.stream(StreamAuthApp.STREAM), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.stream(StreamAuthApp.STREAM2), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE), EnumSet.of(Action.ADMIN))
      .put(AUTH_NAMESPACE.datasetType(KeyValueTable.class.getName()), EnumSet.of(Action.ADMIN))
      .build();
    setUpPrivilegeAndRegisterForDeletion(ALICE, neededPrivileges);
  }

  private void setUpPrivilegeAndRegisterForDeletion(Principal principal,
                                                    Map<EntityId, Set<Action>> neededPrivileges) throws Exception {
    for (Map.Entry<EntityId, Set<Action>> privilege : neededPrivileges.entrySet()) {
      grantAndAssertSuccess(privilege.getKey(), principal, privilege.getValue());
      cleanUpEntities.add(privilege.getKey());
    }
  }
}
