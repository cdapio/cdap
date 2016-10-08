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

package co.cask.cdap.security;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.RunRecord;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
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
import java.util.EnumSet;
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
    Constants.Security.Authorization.CACHE_ENABLED, false
  );
  private static final EnumSet<Action> ALL_ACTIONS = EnumSet.allOf(Action.class);

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId AUTH_NAMESPACE = new NamespaceId("authorization");
  private static final NamespaceMeta AUTH_NAMESPACE_META =
    new NamespaceMeta.Builder().setName(AUTH_NAMESPACE.getNamespace()).build();

  private static InstanceId instance;
  private static String oldUser;

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
      };
    }
  }

  @ClassRule
  public static final AuthTestConf AUTH_TEST_CONF = new AuthTestConf();

  @BeforeClass
  public static void setup() {
    instance = new InstanceId(getConfiguration().get(Constants.INSTANCE_NAME));
    oldUser = SecurityRequestContext.getUserId();
    SecurityRequestContext.setUserId(ALICE.getName());
  }

  @Before
  public void setupTest() throws Exception {
    Assert.assertEquals(ImmutableSet.<Privilege>of(), getAuthorizer().listPrivileges(ALICE));
  }

  @Test
  public void testNamespaces() throws Exception {
    NamespaceAdmin namespaceAdmin = getNamespaceAdmin();
    Authorizer authorizer = getAuthorizer();
    try {
      namespaceAdmin.create(AUTH_NAMESPACE_META);
      Assert.fail("Namespace create should have failed because alice is not authorized on " + instance);
    } catch (UnauthorizedException expected) {
      // expected
    }
    createAuthNamespace();
    // No authorization currently for listing and retrieving namespace
    namespaceAdmin.list();
    namespaceAdmin.get(AUTH_NAMESPACE.toId());
    // revoke privileges
    revokeAndAssertSuccess(AUTH_NAMESPACE);
    try {
      namespaceAdmin.deleteDatasets(AUTH_NAMESPACE.toId());
      Assert.fail("Namespace delete datasets should have failed because alice's privileges on the namespace have " +
                    "been revoked");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant privileges again
    grantAndAssertSuccess(AUTH_NAMESPACE, ALICE, ImmutableSet.of(Action.ADMIN));
    namespaceAdmin.deleteDatasets(AUTH_NAMESPACE.toId());
    // deleting datasets does not revoke privileges.
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(instance, Action.ADMIN), new Privilege(AUTH_NAMESPACE, Action.ADMIN)),
      authorizer.listPrivileges(ALICE)
    );
    NamespaceMeta updated = new NamespaceMeta.Builder(AUTH_NAMESPACE_META).setDescription("new desc").build();
    namespaceAdmin.updateProperties(AUTH_NAMESPACE.toId(), updated);
  }

  @Test
  @Category(SlowTests.class)
  public void testFlowStreamAuth() throws Exception {
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), StreamAuthApp.class);
    // After deploy, change Alice from ALL to ADMIN on the namespace
    authorizer.revoke(AUTH_NAMESPACE, ALICE, EnumSet.allOf(Action.class));
    authorizer.grant(AUTH_NAMESPACE, ALICE, EnumSet.of(Action.ADMIN));

    final FlowManager flowManager = appManager.getFlowManager(StreamAuthApp.FLOW);
    StreamId streamId = AUTH_NAMESPACE.stream(StreamAuthApp.STREAM);
    StreamManager streamManager = getStreamManager(AUTH_NAMESPACE.toId(), StreamAuthApp.STREAM);
    StreamManager streamManager2 = getStreamManager(AUTH_NAMESPACE.toId(), StreamAuthApp.STREAM2);
    streamManager.send("Auth");
    flowManager.start();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> kvTable = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
        return kvTable.get().read("Auth") != null;
      }
    }, 5, TimeUnit.SECONDS);
    flowManager.stop();
    flowManager.waitForFinish(5, TimeUnit.SECONDS);

    // Now revoke read permission for Alice on that stream (revoke ALL and then grant everything other than READ)
    authorizer.revoke(streamId, ALICE, EnumSet.allOf(Action.class));
    authorizer.grant(streamId, ALICE, EnumSet.of(Action.WRITE, Action.ADMIN, Action.EXECUTE));
    streamManager.send("Security");
    streamManager2.send("Safety");
    try {
      flowManager.start();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    authorizer.grant(streamId, ALICE, ImmutableSet.of(Action.READ));
    flowManager.start();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> kvTable = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
        return kvTable.get().read("Security") != null;
      }
    }, 5, TimeUnit.SECONDS);
    authorizer.revoke(streamId, ALICE, ImmutableSet.of(Action.READ));
    TimeUnit.MILLISECONDS.sleep(10);
    flowManager.stop();
    flowManager.waitForFinish(5, TimeUnit.SECONDS);
    appManager.delete();
  }

  @Test
  @Category(SlowTests.class)
  public void testWorkerStreamAuth() throws Exception {
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), StreamAuthApp.class);
    // After deploy, change Alice from ALL to ADMIN on the namespace
    authorizer.revoke(AUTH_NAMESPACE, ALICE, EnumSet.allOf(Action.class));
    authorizer.grant(AUTH_NAMESPACE, ALICE, EnumSet.of(Action.ADMIN));

    WorkerManager workerManager = appManager.getWorkerManager(StreamAuthApp.WORKER);
    workerManager.start();
    workerManager.waitForFinish(5, TimeUnit.SECONDS);
    try {
      workerManager.stop();
    } catch (Exception e) {
      // workaround since we want worker job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }
    StreamId streamId = AUTH_NAMESPACE.stream(StreamAuthApp.STREAM);
    StreamManager streamManager = getStreamManager(AUTH_NAMESPACE.toId(), StreamAuthApp.STREAM);
    Assert.assertEquals(5, streamManager.getEvents(0, Long.MAX_VALUE, Integer.MAX_VALUE).size());

    // Now revoke write permission for Alice on that stream (revoke ALL and then grant everything other than WRITE)
    authorizer.revoke(streamId, ALICE, EnumSet.allOf(Action.class));
    authorizer.grant(streamId, ALICE, EnumSet.of(Action.READ, Action.ADMIN, Action.EXECUTE));
    workerManager.start();
    workerManager.waitForFinish(5, TimeUnit.SECONDS);
    try {
      workerManager.stop();
    } catch (Exception e) {
      // workaround since we want worker job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }
    // Give permissions back so that we can fetch the stream events
    authorizer.grant(streamId, ALICE, EnumSet.allOf(Action.class));
    Assert.assertEquals(5, streamManager.getEvents(0, Long.MAX_VALUE, Integer.MAX_VALUE).size());
    appManager.delete();
    assertNoAccess(AUTH_NAMESPACE.app(StreamAuthApp.APP));
  }

  @Test
  @Category(SlowTests.class)
  public void testSparkStreamAuth() throws Exception {
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    StreamId streamId = AUTH_NAMESPACE.stream(StreamAuthApp.STREAM);
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), StreamAuthApp.class);
    // After deploy, change Alice from ALL to ADMIN on the namespace
    authorizer.revoke(AUTH_NAMESPACE, ALICE, EnumSet.allOf(Action.class));
    authorizer.grant(AUTH_NAMESPACE, ALICE, EnumSet.of(Action.ADMIN));

    StreamManager streamManager = getStreamManager(AUTH_NAMESPACE.toId(), StreamAuthApp.STREAM);
    streamManager.send("Hello");
    final SparkManager sparkManager = appManager.getSparkManager(StreamAuthApp.SPARK);
    sparkManager.start();
    sparkManager.waitForFinish(1, TimeUnit.MINUTES);
    try {
      sparkManager.stop();
    } catch (Exception e) {
      // workaround since we want spark job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }
    DataSetManager<KeyValueTable> kvManager = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("Hello");
      Assert.assertArrayEquals(Bytes.toBytes("Hello"), value);
    }

    streamManager.send("World");
    // Revoke READ permission on STREAM for Alice
    authorizer.revoke(streamId, ALICE, EnumSet.allOf(Action.class));
    authorizer.grant(streamId, ALICE, EnumSet.of(Action.WRITE, Action.ADMIN, Action.EXECUTE));
    sparkManager.start();
    sparkManager.waitForFinish(1, TimeUnit.MINUTES);
    try {
      sparkManager.stop();
    } catch (Exception e) {
      // workaround since we want spark job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }

    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return sparkManager.getHistory(ProgramRunStatus.FAILED).size();
      }
    }, 5, TimeUnit.SECONDS);
    kvManager = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("World");
      Assert.assertNull(value);
    }

    // Grant ALICE, READ permission on STREAM and now Spark job should run successfully
    authorizer.grant(streamId, ALICE, ImmutableSet.of(Action.READ));
    sparkManager.start();
    sparkManager.waitForFinish(1, TimeUnit.MINUTES);
    try {
      sparkManager.stop();
    } catch (Exception e) {
      // workaround since we want spark job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }

    // so far there should be 2 successful runs of spark program
    Tasks.waitFor(2, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return sparkManager.getHistory(ProgramRunStatus.COMPLETED).size();
      }
    }, 5, TimeUnit.SECONDS);
    kvManager = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("World");
      Assert.assertArrayEquals(Bytes.toBytes("World"), value);
    }
    appManager.delete();
    assertNoAccess(AUTH_NAMESPACE.app(StreamAuthApp.APP));
  }

  @Test
  @Category(SlowTests.class)
  public void testMRStreamAuth() throws Exception {
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), StreamAuthApp.class);
    // After deploy, change Alice from ALL to ADMIN on the namespace
    authorizer.revoke(AUTH_NAMESPACE, ALICE, EnumSet.allOf(Action.class));
    authorizer.grant(AUTH_NAMESPACE, ALICE, EnumSet.of(Action.ADMIN));

    StreamManager streamManager = getStreamManager(AUTH_NAMESPACE.toId(), StreamAuthApp.STREAM);
    streamManager.send("Hello");
    final MapReduceManager mrManager = appManager.getMapReduceManager(StreamAuthApp.MAPREDUCE);
    mrManager.start();
    mrManager.waitForFinish(1, TimeUnit.MINUTES);
    try {
      mrManager.stop();
    } catch (Exception e) {
      // workaround since we want mr job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }
    DataSetManager<KeyValueTable> kvManager = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("Hello");
      Assert.assertArrayEquals(Bytes.toBytes("Hello"), value);
    }
    // Since Alice had full permissions, she should be able to execute the MR job successfully
    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return mrManager.getHistory(ProgramRunStatus.COMPLETED).size();
      }
    }, 5, TimeUnit.SECONDS);

    ProgramId mrId = AUTH_NAMESPACE.app(StreamAuthApp.APP).mr(StreamAuthApp.MAPREDUCE);
    authorizer.grant(mrId.getNamespaceId(), BOB, ImmutableSet.of(Action.ADMIN));
    ArtifactSummary artifactSummary = appManager.getInfo().getArtifact();

    ArtifactId artifactId = AUTH_NAMESPACE.artifact(artifactSummary.getName(), artifactSummary.getVersion());
    authorizer.grant(artifactId, BOB, EnumSet.allOf(Action.class));
    authorizer.grant(mrId.getParent(), BOB, EnumSet.allOf(Action.class));
    authorizer.grant(mrId, BOB, EnumSet.allOf(Action.class));
    authorizer.grant(AUTH_NAMESPACE.stream(StreamAuthApp.STREAM), BOB, EnumSet.of(Action.ADMIN));
    authorizer.grant(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE), BOB, EnumSet.allOf(Action.class));
    streamManager.send("World");

    // Switch user to Bob. Note that he doesn't have READ access on the stream.
    SecurityRequestContext.setUserId(BOB.getName());
    mrManager.start();
    mrManager.waitForFinish(1, TimeUnit.MINUTES);
    try {
      mrManager.stop();
    } catch (Exception e) {
      // workaround since we want mr job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }

    Tasks.waitFor(1, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return mrManager.getHistory(ProgramRunStatus.FAILED).size();
      }
    }, 5, TimeUnit.SECONDS);
    kvManager = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("World");
      Assert.assertNull(value);
    }

    // Now grant Bob, READ access on the stream. MR job should execute successfully now.
    authorizer.grant(AUTH_NAMESPACE.stream(StreamAuthApp.STREAM), BOB, ImmutableSet.of(Action.READ));
    mrManager.start();
    mrManager.waitForFinish(1, TimeUnit.MINUTES);
    try {
      mrManager.stop();
    } catch (Exception e) {
      // workaround since we want mr job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }

    Tasks.waitFor(2, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return mrManager.getHistory(ProgramRunStatus.COMPLETED).size();
      }
    }, 5, TimeUnit.SECONDS);
    kvManager = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
    try (KeyValueTable kvTable = kvManager.get()) {
      byte[] value = kvTable.read("World");
      Assert.assertEquals("World", Bytes.toString(value));
    }

    SecurityRequestContext.setUserId(ALICE.getName());
    appManager.delete();
    assertNoAccess(AUTH_NAMESPACE.app(StreamAuthApp.APP));
  }

  @Test
  @Category(SlowTests.class)
  public void testApps() throws Exception {
    try {
      deployApplication(NamespaceId.DEFAULT.toId(), DummyApp.class);
      Assert.fail("App deployment should fail because alice does not have WRITE access on the default namespace");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }
    createAuthNamespace();
    Authorizer authorizer = getAuthorizer();
    // deployment should succeed in the authorized namespace because alice has all privileges on it
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), DummyApp.class);
    // alice should get all privileges on the app after deployment succeeds
    ApplicationId dummyAppId = AUTH_NAMESPACE.app(DummyApp.class.getSimpleName());
    ArtifactSummary artifact = appManager.getInfo().getArtifact();
    ArtifactId dummyArtifact =
      Ids.namespace(dummyAppId.getNamespace()).artifact(artifact.getName(), artifact.getVersion());
    ProgramId greetingServiceId = dummyAppId.service(DummyApp.Greeting.SERVICE_NAME);
    DatasetId dsId = AUTH_NAMESPACE.dataset("whom");
    StreamId streamId = AUTH_NAMESPACE.stream("who");
    assertAllAccess(ALICE, AUTH_NAMESPACE, dummyAppId, dummyArtifact, greetingServiceId, dsId, streamId);
    // Bob should not have any privileges on Alice's app
    Assert.assertTrue("Bob should not have any privileges on alice's app", authorizer.listPrivileges(BOB).isEmpty());
    // This is necessary because in tests, artifacts have auto-generated versions when an app is deployed without
    // first creating an artifact
    String version = artifact.getVersion();
    // update should succeed because alice has admin privileges on the app
    appManager.update(new AppRequest(artifact));
    // Update should fail for Bob
    SecurityRequestContext.setUserId(BOB.getName());
    try {
      appManager.update(new AppRequest(new ArtifactSummary(DummyApp.class.getSimpleName(), version)));
      Assert.fail("App update should have failed because Alice does not have admin privileges on the app.");
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
    try {
      appManager.delete();
      Assert.fail("Deletion should have failed since Bob don't have any privileges to namespace");
    } catch (Exception expected) {
      // expected
    }

    grantAndAssertSuccess(AUTH_NAMESPACE, BOB, ImmutableSet.of(Action.READ));
    appManager.delete();

    // All privileges on the app should have been revoked
    Assert.assertFalse(getAuthorizer().createFilter(BOB).apply(dummyAppId));
    assertAllAccess(ALICE, AUTH_NAMESPACE, dummyArtifact, dsId, streamId);

    authorizer.revoke(AUTH_NAMESPACE, BOB, ImmutableSet.of(Action.READ));
    Assert.assertTrue("Bob should not have any privileges because all privileges on the app have been revoked " +
                        "since the app got deleted", authorizer.listPrivileges(BOB).isEmpty());
    // switch back to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // Deploy a couple of apps in the namespace
    appManager = deployApplication(AUTH_NAMESPACE.toId(), DummyApp.class);
    artifact = appManager.getInfo().getArtifact();
    ArtifactId updatedDummyArtifact = AUTH_NAMESPACE.artifact(artifact.getName(), artifact.getVersion());
    appManager = deployApplication(AUTH_NAMESPACE.toId(), AllProgramsApp.class);
    artifact = appManager.getInfo().getArtifact();
    ArtifactId workflowArtifact = AUTH_NAMESPACE.artifact(artifact.getName(), artifact.getVersion());
    ApplicationId workflowAppId = AUTH_NAMESPACE.app(AllProgramsApp.NAME);

    ProgramId flowId = workflowAppId.flow(AllProgramsApp.NoOpFlow.NAME);
    ProgramId classicMapReduceId = workflowAppId.mr(AllProgramsApp.NoOpMR.NAME);
    ProgramId mapReduceId = workflowAppId.mr(AllProgramsApp.NoOpMR2.NAME);
    ProgramId sparkId = workflowAppId.spark(AllProgramsApp.NoOpSpark.NAME);
    ProgramId workflowId = workflowAppId.workflow(AllProgramsApp.NoOpWorkflow.NAME);
    ProgramId serviceId = workflowAppId.service(AllProgramsApp.NoOpService.NAME);
    ProgramId workerId = workflowAppId.worker(AllProgramsApp.NoOpWorker.NAME);
    DatasetId kvt = AUTH_NAMESPACE.dataset(AllProgramsApp.DATASET_NAME);
    DatasetId kvt2 = AUTH_NAMESPACE.dataset(AllProgramsApp.DATASET_NAME2);
    DatasetId kvt3 = AUTH_NAMESPACE.dataset(AllProgramsApp.DATASET_NAME3);
    DatasetId dsWithSchema = AUTH_NAMESPACE.dataset(AllProgramsApp.DS_WITH_SCHEMA_NAME);
    StreamId sId = AUTH_NAMESPACE.stream(AllProgramsApp.STREAM_NAME);

    assertAllAccess(
      ALICE, AUTH_NAMESPACE, dummyArtifact, updatedDummyArtifact, workflowArtifact, dummyAppId, streamId, workflowAppId,
      greetingServiceId, flowId, classicMapReduceId, mapReduceId, sparkId, workflowId, serviceId, workerId, dsId,
      kvt, kvt2, kvt3, dsWithSchema, sId
    );
    // revoke all privileges on an app.
    authorizer.revoke(workflowAppId);
    // TODO: CDAP-5428 Revoking privileges on an app should revoke privileges on the contents of the app
    authorizer.revoke(flowId);
    authorizer.revoke(classicMapReduceId);
    authorizer.revoke(mapReduceId);
    authorizer.revoke(sparkId);
    authorizer.revoke(workflowId);
    authorizer.revoke(serviceId);
    authorizer.revoke(workerId);

    assertAllAccess(
      ALICE, AUTH_NAMESPACE, dummyArtifact, updatedDummyArtifact, workflowArtifact, dummyAppId, streamId,
      greetingServiceId, dsId, kvt, kvt2, kvt3, dsWithSchema, sId
    );

    // Revoke Alice all access, but granting her READ, WRITE and EXECUTE
    authorizer.revoke(AUTH_NAMESPACE, ALICE, EnumSet.allOf(Action.class));
    grantAndAssertSuccess(AUTH_NAMESPACE, ALICE, EnumSet.of(Action.READ, Action.WRITE, Action.EXECUTE));

    // deleting all apps should fail because alice does not have admin privileges on the Workflow app and the namespace
    try {
      deleteAllApplications(AUTH_NAMESPACE);
      Assert.fail("Deleting all applications in the namespace should have failed because alice does not have ADMIN " +
                    "privilege on the workflow app.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant admin privilege on the WorkflowApp. deleting all applications should succeed.
    grantAndAssertSuccess(workflowAppId, ALICE, ImmutableSet.of(Action.ADMIN));
    deleteAllApplications(AUTH_NAMESPACE);
    // deleting all apps should remove all privileges on all apps, but the privilege on the namespace should still exist
    getAuthorizer().enforce(AUTH_NAMESPACE, ALICE, EnumSet.of(Action.READ, Action.WRITE, Action.EXECUTE));
    assertAllAccess(
      ALICE, dummyArtifact, updatedDummyArtifact, workflowArtifact, streamId, dsId, kvt, kvt2, kvt3, dsWithSchema, sId
    );
  }

  @Test
  public void testArtifacts() throws Exception {
    String appArtifactName = "app-artifact";
    String appArtifactVersion = "1.1.1";
    try {
      ArtifactId defaultNsArtifact = NamespaceId.DEFAULT.artifact(appArtifactName, appArtifactVersion);
      addAppArtifact(defaultNsArtifact, ConfigTestApp.class);
      Assert.fail("Should not be able to add an app artifact to the default namespace because alice does not have " +
                    "write privileges on the default namespace.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    String pluginArtifactName = "plugin-artifact";
    String pluginArtifactVersion = "1.2.3";
    try {
      ArtifactId defaultNsArtifact = NamespaceId.DEFAULT.artifact(pluginArtifactName, pluginArtifactVersion);
      addAppArtifact(defaultNsArtifact, ToStringPlugin.class);
      Assert.fail("Should not be able to add a plugin artifact to the default namespace because alice does not have " +
                    "write privileges on the default namespace.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // create a new namespace, alice should get ALL privileges on the namespace
    createAuthNamespace();
    // artifact deployment in this namespace should now succeed, and alice should have ALL privileges on the artifacts
    ArtifactId appArtifactId = AUTH_NAMESPACE.artifact(appArtifactName, appArtifactVersion);
    ArtifactManager appArtifactManager = addAppArtifact(appArtifactId, ConfigTestApp.class);
    ArtifactId pluginArtifactId = AUTH_NAMESPACE.artifact(pluginArtifactName, pluginArtifactVersion);
    ArtifactManager pluginArtifactManager = addPluginArtifact(pluginArtifactId, appArtifactId, ToStringPlugin.class);
    assertAllAccess(ALICE, AUTH_NAMESPACE, appArtifactId, pluginArtifactId);
    // Bob should not be able to delete artifacts that he does not have ADMIN permission on
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
    // upon successful deletion, alice should lose all privileges on the artifact
    assertNoAccess(appArtifactId);
    assertNoAccess(pluginArtifactId);
  }

  @Test
  public void testPrograms() throws Exception {
    createAuthNamespace();
    final ApplicationManager dummyAppManager = deployApplication(AUTH_NAMESPACE.toId(), DummyApp.class);
    ArtifactSummary dummyArtifactSummary = dummyAppManager.getInfo().getArtifact();
    ArtifactId dummyArtifact = AUTH_NAMESPACE.artifact(dummyArtifactSummary.getName(),
                                                       dummyArtifactSummary.getVersion());
    ApplicationId appId = AUTH_NAMESPACE.app(DummyApp.class.getSimpleName());
    final ProgramId serviceId = appId.service(DummyApp.Greeting.SERVICE_NAME);
    DatasetId dsId = AUTH_NAMESPACE.dataset("whom");
    StreamId streamId = AUTH_NAMESPACE.stream("who");
    assertAllAccess(ALICE, AUTH_NAMESPACE, dummyArtifact, appId, serviceId, dsId, streamId);
    // alice should be able to start and stop programs in the app she deployed
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
      Assert.fail("Bob should not be able to start the service because he does not have admin privileges on it.");
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
    SecurityRequestContext.setUserId(ALICE.getName());
    dummyAppManager.delete();
    assertNoAccess(appId);
  }

  @Test
  public void testCrossNSFlowlet() throws Exception {
    createAuthNamespace();
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), CrossNsDatasetAccessApp.class);

    // give BOB ALL permissions on the auth namespace so he can execute programs and also read the stream.
    grantAndAssertSuccess(AUTH_NAMESPACE, BOB, EnumSet.allOf(Action.class));

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());

    // Send data to stream as BOB this ensures that BOB can write to a stream in auth namespace
    StreamManager streamManager = getStreamManager(AUTH_NAMESPACE.toId(), CrossNsDatasetAccessApp.STREAM_NAME);
    for (int i = 0; i < 10; i++) {
      streamManager.send(String.valueOf(i).getBytes());
    }

    // switch to back to ALICE
    SecurityRequestContext.setUserId(ALICE.getName());

    final FlowManager flowManager = appManager.getFlowManager(CrossNsDatasetAccessApp.FLOW_NAME);

    testSystemDatasetAccessFromFlowlet(flowManager);
    testCrossNSDatasetAccessFromFlowlet(flowManager);

    appManager.stopAll();
  }

  private void testSystemDatasetAccessFromFlowlet(final FlowManager flowManager) throws Exception {
    addDatasetInstance(Id.Namespace.SYSTEM, "keyValueTable", "store");

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
    deleteDatasetInstance(NamespaceId.SYSTEM, "store");
  }

  private void testCrossNSDatasetAccessFromFlowlet(final FlowManager flowManager) throws Exception {
    NamespaceMeta outputDatasetNS = new NamespaceMeta.Builder().setName("outputNS").build();
    getNamespaceAdmin().create(outputDatasetNS);
    addDatasetInstance(outputDatasetNS.getNamespaceId().toId(), "keyValueTable", "store");

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
    grantAndAssertSuccess(outputDatasetNS.getNamespaceId().dataset("store"), BOB, EnumSet.of(Action.WRITE));

    // switch back to BOB to run flow again
    SecurityRequestContext.setUserId(BOB.getName());

    // running the flow now should pass and write data in another namespace successfully
    flowManager.start(args);
    flowManager.getFlowletMetrics("saver").waitForProcessed(10, 30, TimeUnit.SECONDS);

    // switch back to alice and verify the data its fine now to verify the run record here because if the flow failed
    // to write we will not see any data
    SecurityRequestContext.setUserId(ALICE.getName());

    DataSetManager<KeyValueTable> dataSetManager = getDataset(outputDatasetNS.getNamespaceId().toId(), "store");
    KeyValueTable results = dataSetManager.get();

    for (int i = 0; i < 10; i++) {
      byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
      Assert.assertArrayEquals(key, results.read(key));
    }
    flowManager.stop();
    getNamespaceAdmin().delete(outputDatasetNS.getNamespaceId().toId());
  }

  @Test
  public void testCrossNSMapReduce() throws Exception {
    createAuthNamespace();
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), DatasetCrossNSAccessWithMAPApp.class);

    // give BOB ALL permission in the
    grantAndAssertSuccess(AUTH_NAMESPACE, BOB, EnumSet.allOf(Action.class));

    MapReduceManager mrManager = appManager.getMapReduceManager(DatasetCrossNSAccessWithMAPApp.MAPREDUCE_PROGRAM);

    testCrossNSSystemDatasetAccessWithAuthMapReduce(mrManager);
    testCrossNSDatasetAccessWithAuthMapReduce(mrManager);

    appManager.stopAll();
  }

  private void testCrossNSSystemDatasetAccessWithAuthMapReduce(MapReduceManager mrManager) throws Exception {
    addDatasetInstance(Id.Namespace.SYSTEM, "keyValueTable", "table1").create();
    addDatasetInstance(Id.Namespace.SYSTEM, "keyValueTable", "table2").create();
    NamespaceMeta otherNS = new NamespaceMeta.Builder().setName("otherNS").build();
    getNamespaceAdmin().create(otherNS);
    addDatasetInstance(otherNS.getNamespaceId().toId(), "keyValueTable", "otherTable").create();
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
    deleteDatasetInstance(NamespaceId.SYSTEM, "table1");
    deleteDatasetInstance(NamespaceId.SYSTEM, "table2");
    getNamespaceAdmin().delete(otherNS.getNamespaceId().toId());
  }

  private void testCrossNSDatasetAccessWithAuthMapReduce(MapReduceManager mrManager) throws Exception {
    NamespaceMeta inputDatasetNS = new NamespaceMeta.Builder().setName("inputNS").build();
    getNamespaceAdmin().create(inputDatasetNS);
    NamespaceMeta outputDatasetNS = new NamespaceMeta.Builder().setName("outputNS").build();
    getNamespaceAdmin().create(outputDatasetNS);
    addDatasetInstance(inputDatasetNS.getNamespaceId().toId(), "keyValueTable", "table1").create();
    addDatasetInstance(outputDatasetNS.getNamespaceId().toId(), "keyValueTable", "table2").create();

    addDummyData(inputDatasetNS.getNamespaceId(), "table1");

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
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    // Verify results as alice
    SecurityRequestContext.setUserId(ALICE.getName());
    verifyDummyData(outputDatasetNS.getNamespaceId(), "table2");
    getNamespaceAdmin().delete(inputDatasetNS.getNamespaceId().toId());
    getNamespaceAdmin().delete(outputDatasetNS.getNamespaceId().toId());
  }

  @Test
  public void testCrossNSSpark() throws Exception {
    createAuthNamespace();

    // give BOB ALL permission on the auth namespace
    grantAndAssertSuccess(AUTH_NAMESPACE, BOB, ALL_ACTIONS);

    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), TestSparkCrossNSDatasetApp.class);
    SparkManager sparkManager = appManager.getSparkManager(TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram
                                                             .class.getSimpleName());

    testCrossNSSystemDatasetAccessWithAuthSpark(sparkManager);
    testCrossNSDatasetAccessWithAuthSpark(sparkManager);

    appManager.stopAll();
  }

  @Test
  public void testScheduleAuth() throws Exception {
    createAuthNamespace();
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), AppWithSchedule.class);
    ProgramId workflowID = new ProgramId(AUTH_NAMESPACE.getNamespace(), AppWithSchedule.class.getSimpleName(),
                                         ProgramType.WORKFLOW, AppWithSchedule.SampleWorkflow.class.getSimpleName());

    final WorkflowManager workflowManager =
      appManager.getWorkflowManager(AppWithSchedule.SampleWorkflow.class.getSimpleName());
    ScheduleManager scheduleManager = workflowManager.getSchedule(AppWithSchedule.SCHEDULE_NAME);

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());
    // try to resume schedule as BOB. It should fail since BOB does not have privileges on the programs
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
    // try to resume schedule as BOB. It should fail since BOB has READ and not EXECUTE on the workflow
    try {
      scheduleManager.resume();
      Assert.fail("Resuming schedule should have failed since BOB does not have EXECUTE on the program");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    // but BOB should be able to get schedule status now
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED.name(), scheduleManager.status(HttpURLConnection.HTTP_OK));

    // switch to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // give BOB EXECUTE permission in the workflow
    grantAndAssertSuccess(workflowID, BOB, EnumSet.of(Action.EXECUTE));

    // switch to BOB
    SecurityRequestContext.setUserId(BOB.getName());
    // try to resume the schedule. This should pass and workflow should run
    scheduleManager.resume();
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED.name(), scheduleManager.status(HttpURLConnection.HTTP_OK));

    // wait for workflow to start
    workflowManager.waitForStatus(true);

    // suspend the schedule so that it does not start running again
    scheduleManager.suspend();

    // wait for scheduled runs of workflow to run to end
    workflowManager.waitForStatus(false, 2 , 3);

    // since the schedule in AppWithSchedule is to  run every second its possible that it will trigger more than one
    // run before the schedule was suspended so check for greater than 0 rather than equal to 1
    Assert.assertTrue(0 < workflowManager.getHistory().size());
    // assert that all run completed
    for (RunRecord runRecord : workflowManager.getHistory()) {
      Assert.assertEquals(ProgramRunStatus.COMPLETED, runRecord.getStatus());
    }

    // switch to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
  }

  private void testCrossNSSystemDatasetAccessWithAuthSpark(SparkManager sparkManager) throws Exception {
    addDatasetInstance(Id.Namespace.SYSTEM, "keyValueTable", "table1").create();
    addDatasetInstance(Id.Namespace.SYSTEM, "keyValueTable", "table2").create();
    NamespaceMeta otherNS = new NamespaceMeta.Builder().setName("otherNS").build();
    getNamespaceAdmin().create(otherNS);
    addDatasetInstance(otherNS.getNamespaceId().toId(), "keyValueTable", "otherTable").create();
    addDummyData(NamespaceId.SYSTEM, "table1");

    // give privilege to BOB on all the datasets
    grantAndAssertSuccess(NamespaceId.SYSTEM.dataset("table1"), BOB, EnumSet.of(Action.READ));
    grantAndAssertSuccess(NamespaceId.SYSTEM.dataset("table2"), BOB, EnumSet.of(Action.WRITE));
    grantAndAssertSuccess(otherNS.getNamespaceId().dataset("otherTable"), BOB, ALL_ACTIONS);

    // Switch to Bob and run the spark program. this will fail because bob is trying to read from a system dataset
    SecurityRequestContext.setUserId(BOB.getName());
    Map<String, String> args = ImmutableMap.of(
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.INPUT_DATASET_NAMESPACE,
      NamespaceId.SYSTEM.getNamespace(),
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.INPUT_DATASET_NAME, "table1",
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.OUTPUT_DATASET_NAMESPACE,
      otherNS.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.OUTPUT_DATASET_NAME, "otherTable"
    );

    assertProgramFailure(args, sparkManager);
    assertDatasetIsEmpty(otherNS.getNamespaceId(), "otherTable");

    // try running spark job with valid input namespace but writing to system namespace this should fail too
    args = ImmutableMap.of(
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.INPUT_DATASET_NAMESPACE,
      otherNS.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.INPUT_DATASET_NAME, "otherTable",
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.OUTPUT_DATASET_NAMESPACE,
      NamespaceId.SYSTEM.getNamespace(),
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.OUTPUT_DATASET_NAME, "table2"
    );

    addDummyData(otherNS.getNamespaceId(), "otherTable");

    assertProgramFailure(args, sparkManager);
    assertDatasetIsEmpty(NamespaceId.SYSTEM, "table2");

    // switch to back to ALICE
    SecurityRequestContext.setUserId(ALICE.getName());

    // cleanup
    deleteDatasetInstance(NamespaceId.SYSTEM, "table1");
    deleteDatasetInstance(NamespaceId.SYSTEM, "table2");
    getNamespaceAdmin().delete(otherNS.getNamespaceId().toId());
  }

  private void testCrossNSDatasetAccessWithAuthSpark(SparkManager sparkManager) throws Exception {
    NamespaceMeta inputDatasetNSMeta = new NamespaceMeta.Builder().setName("inputDatasetNS").build();
    NamespaceMeta outputDatasetNSMeta = new NamespaceMeta.Builder().setName("outputDatasetNS").build();
    getNamespaceAdmin().create(inputDatasetNSMeta);
    getNamespaceAdmin().create(outputDatasetNSMeta);
    addDatasetInstance(inputDatasetNSMeta.getNamespaceId().toId(), "keyValueTable", "input").create();
    addDatasetInstance(outputDatasetNSMeta.getNamespaceId().toId(), "keyValueTable", "output").create();
    // write sample stuff in input dataset
    addDummyData(inputDatasetNSMeta.getNamespaceId(), "input");

    // Switch to Bob and run the spark program. this will fail because bob does not have access to either input or
    // output dataset
    SecurityRequestContext.setUserId(BOB.getName());
    Map<String, String> args = ImmutableMap.of(
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.INPUT_DATASET_NAMESPACE,
      inputDatasetNSMeta.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.INPUT_DATASET_NAME, "input",
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.OUTPUT_DATASET_NAMESPACE,
      outputDatasetNSMeta.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.OUTPUT_DATASET_NAME, "output"
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
    sparkManager.waitForFinish(120, TimeUnit.SECONDS);

    // Verify the results as alice
    SecurityRequestContext.setUserId(ALICE.getName());
    verifyDummyData(outputDatasetNSMeta.getNamespaceId(), "output");
    getNamespaceAdmin().delete(inputDatasetNSMeta.getNamespaceId().toId());
    getNamespaceAdmin().delete(outputDatasetNSMeta.getNamespaceId().toId());
  }

  @Test
  public void testAddDropPartitions() throws Exception {
    createAuthNamespace();
    ApplicationManager appMgr = deployApplication(AUTH_NAMESPACE.toId(), PartitionTestApp.class);
    grantAndAssertSuccess(AUTH_NAMESPACE, BOB, EnumSet.of(Action.READ, Action.EXECUTE));
    SecurityRequestContext.setUserId(BOB.getName());
    String partition = "p1";
    String subPartition = "1";
    String text = "some random text for pfs";
    ServiceManager pfsService = appMgr.getServiceManager(PartitionTestApp.PFS_SERVICE_NAME);
    pfsService.start();
    pfsService.waitForStatus(true);
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
      pfsService.waitForFinish(5, TimeUnit.SECONDS);
    }
    // grant write on dataset and restart
    grantAndAssertSuccess(AUTH_NAMESPACE.dataset(PartitionTestApp.PFS_NAME), BOB, EnumSet.of(Action.WRITE));
    pfsService.start();
    pfsService.waitForStatus(true);
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
      pfsService.waitForFinish(5, TimeUnit.SECONDS);
      SecurityRequestContext.setUserId(ALICE.getName());
    }
  }

  @After
  public void cleanupTest() throws Exception {
    Authorizer authorizer = getAuthorizer();

    grantAndAssertSuccess(AUTH_NAMESPACE, SecurityRequestContext.toPrincipal(), EnumSet.allOf(Action.class));
    // clean up. remove the namespace. all privileges on the namespace should be revoked
    getNamespaceAdmin().delete(AUTH_NAMESPACE.toId());
    Assert.assertEquals(ImmutableSet.of(new Privilege(instance, Action.ADMIN)), authorizer.listPrivileges(ALICE));
    // revoke privileges on the instance
    revokeAndAssertSuccess(instance);
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
    grantAndAssertSuccess(instance, ALICE, ImmutableSet.of(Action.ADMIN));
    getNamespaceAdmin().create(AUTH_NAMESPACE_META);
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(instance, Action.ADMIN), new Privilege(AUTH_NAMESPACE, Action.ADMIN),
                      new Privilege(AUTH_NAMESPACE, Action.READ), new Privilege(AUTH_NAMESPACE, Action.WRITE),
                      new Privilege(AUTH_NAMESPACE, Action.EXECUTE)),
      authorizer.listPrivileges(ALICE)
    );
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

  private void assertNoAccess(final EntityId entityId) throws Exception {
    Authorizer authorizer = getAuthorizer();
    Predicate<Privilege> entityFilter = new Predicate<Privilege>() {
      @Override
      public boolean apply(Privilege input) {
        return entityId.equals(input.getEntity());
      }
    };
    Assert.assertTrue(Sets.filter(authorizer.listPrivileges(ALICE), entityFilter).isEmpty());
    Assert.assertTrue(Sets.filter(authorizer.listPrivileges(BOB), entityFilter).isEmpty());
  }

  private void assertDatasetIsEmpty(NamespaceId namespaceId, String datasetName) throws Exception {
    DataSetManager<KeyValueTable> outTableManager = getDataset(namespaceId.toId(), datasetName);
    KeyValueTable outputTable = outTableManager.get();
    try (CloseableIterator<KeyValue<byte[], byte[]>> scanner = outputTable.scan(null, null)) {
      Assert.assertFalse(scanner.hasNext());
    }
  }

  private <T extends ProgramManager> void assertProgramFailure(
    Map<String, String> programArgs, final ProgramManager<T> programManager)
    throws TimeoutException, InterruptedException, ExecutionException {
    programManager.start(programArgs);
    programManager.waitForFinish(5, TimeUnit.MINUTES);

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
        return true;
      }
    }, 120, TimeUnit.SECONDS, "Not all program runs have failed status. Expected all run status to be failed");
  }


  private void assertAllAccess(Principal principal, EntityId... entityIds) throws Exception {
    for (EntityId entityId : entityIds) {
      getAuthorizer().enforce(entityId, principal, EnumSet.allOf(Action.class));
    }
  }

  private void addDummyData(NamespaceId namespaceId, String datasetName) throws Exception {
    DataSetManager<KeyValueTable> tableManager = getDataset(namespaceId.toId(), datasetName);
    KeyValueTable inputTable = tableManager.get();
    inputTable.write("hello", "world");
    tableManager.flush();
  }

  private void verifyDummyData(NamespaceId namespaceId, String datasetName) throws Exception {
    DataSetManager<KeyValueTable> outTableManager = getDataset(namespaceId.toId(), datasetName);
    KeyValueTable outputTable = outTableManager.get();
    Assert.assertEquals("world", Bytes.toString(outputTable.read("hello")));
  }
}
