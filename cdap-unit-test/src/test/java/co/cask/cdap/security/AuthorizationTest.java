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
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
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
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ArtifactManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkerManager;
import co.cask.cdap.test.app.DummyApp;
import co.cask.cdap.test.app.StreamAuthApp;
import co.cask.cdap.test.artifacts.plugins.ToStringPlugin;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
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
      return new String[] {
        Constants.Security.ENABLED, "true",
        Constants.Security.Authorization.ENABLED, "true",
        Constants.Security.Authorization.EXTENSION_JAR_PATH, authExtensionJar.toURI().getPath(),
        // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
        Constants.Security.KERBEROS_ENABLED, "false",
        Constants.Security.Authorization.SUPERUSERS, "hulk"
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
    authorizer.revoke(streamId, ALICE, ImmutableSet.of(Action.ALL));
    authorizer.grant(streamId, ALICE, ImmutableSet.of(Action.WRITE, Action.ADMIN, Action.EXECUTE));
    streamManager.send("Security");
    streamManager2.send("Safety");
    flowManager.start();
    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          DataSetManager<KeyValueTable> kvTable = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
          return kvTable.get().read("Security") != null;
        }
      }, 3, TimeUnit.SECONDS);
      Assert.fail("'Security' StreamEvent should not have been processed.");
    } catch (TimeoutException ex) {
      // expected
    }
    // wait for the stream event from stream2 to which Alice has all access
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> kvTable = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
        return kvTable.get().read("Safety") != null;
      }
    }, 3, TimeUnit.SECONDS);
    flowManager.stop();
    flowManager.waitForFinish(5, TimeUnit.SECONDS);

    authorizer.grant(streamId, ALICE, ImmutableSet.of(Action.READ));
    flowManager.start();
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        DataSetManager<KeyValueTable> kvTable = getDataset(AUTH_NAMESPACE.toId(), StreamAuthApp.KVTABLE);
        return kvTable.get().read("Security") != null;
      }
    }, 5, TimeUnit.SECONDS);
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
    authorizer.revoke(streamId, ALICE, ImmutableSet.of(Action.ALL));
    authorizer.grant(streamId, ALICE, ImmutableSet.of(Action.READ, Action.ADMIN, Action.EXECUTE));
    workerManager.start();
    workerManager.waitForFinish(5, TimeUnit.SECONDS);
    try {
      workerManager.stop();
    } catch (Exception e) {
      // workaround since we want worker job to be de-listed from the running processes to allow cleanup to happen
      Assert.assertTrue(e.getCause() instanceof BadRequestException);
    }
    // Give permissions back so that we can fetch the stream events
    authorizer.grant(streamId, ALICE, ImmutableSet.of(Action.ALL));
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
    authorizer.revoke(streamId, ALICE, ImmutableSet.of(Action.ALL));
    authorizer.grant(streamId, ALICE, ImmutableSet.of(Action.WRITE, Action.ADMIN, Action.EXECUTE));
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
    authorizer.grant(mrId.getNamespaceId(), BOB, ImmutableSet.of(Action.ALL));
    ArtifactSummary artifactSummary = appManager.getInfo().getArtifact();

    ArtifactId artifactId = AUTH_NAMESPACE.artifact(artifactSummary.getName(), artifactSummary.getVersion());
    authorizer.grant(artifactId, BOB, ImmutableSet.of(Action.ALL));
    authorizer.grant(mrId.getParent(), BOB, ImmutableSet.of(Action.ALL));
    authorizer.grant(mrId, BOB, ImmutableSet.of(Action.ALL));
    authorizer.grant(AUTH_NAMESPACE.stream(StreamAuthApp.STREAM), BOB, ImmutableSet.of(Action.ADMIN));
    authorizer.grant(AUTH_NAMESPACE.dataset(StreamAuthApp.KVTABLE), BOB, ImmutableSet.of(Action.ALL));
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
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyAppId, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(greetingServiceId, Action.ALL),
        new Privilege(dsId, Action.ALL),
        new Privilege(streamId, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
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
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(dsId, Action.ALL),
        new Privilege(streamId, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );

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

    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(updatedDummyArtifact, Action.ALL),
        new Privilege(workflowArtifact, Action.ALL),
        new Privilege(dummyAppId, Action.ALL),
        new Privilege(streamId, Action.ALL),
        new Privilege(workflowAppId, Action.ALL),
        new Privilege(greetingServiceId, Action.ALL),
        new Privilege(flowId, Action.ALL),
        new Privilege(classicMapReduceId, Action.ALL),
        new Privilege(mapReduceId, Action.ALL),
        new Privilege(sparkId, Action.ALL),
        new Privilege(workflowId, Action.ALL),
        new Privilege(serviceId, Action.ALL),
        new Privilege(workerId, Action.ALL),
        new Privilege(dsId, Action.ALL),
        new Privilege(kvt, Action.ALL),
        new Privilege(kvt2, Action.ALL),
        new Privilege(kvt3, Action.ALL),
        new Privilege(dsWithSchema, Action.ALL),
        new Privilege(sId, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
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

    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(updatedDummyArtifact, Action.ALL),
        new Privilege(workflowArtifact, Action.ALL),
        new Privilege(dummyAppId, Action.ALL),
        new Privilege(streamId, Action.ALL),
        new Privilege(greetingServiceId, Action.ALL),
        new Privilege(dsId, Action.ALL),
        new Privilege(kvt, Action.ALL),
        new Privilege(kvt2, Action.ALL),
        new Privilege(kvt3, Action.ALL),
        new Privilege(dsWithSchema, Action.ALL),
        new Privilege(sId, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // deleting all apps should fail because alice does not have admin privileges on the Workflow app
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
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(streamId, Action.ALL),
        new Privilege(updatedDummyArtifact, Action.ALL),
        new Privilege(workflowArtifact, Action.ALL),
        new Privilege(dsId, Action.ALL),
        new Privilege(kvt, Action.ALL),
        new Privilege(kvt2, Action.ALL),
        new Privilege(kvt3, Action.ALL),
        new Privilege(dsWithSchema, Action.ALL),
        new Privilege(sId, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
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
    Authorizer authorizer = getAuthorizer();
    // artifact deployment in this namespace should now succeed, and alice should have ALL privileges on the artifacts
    ArtifactId appArtifactId = AUTH_NAMESPACE.artifact(appArtifactName, appArtifactVersion);
    ArtifactManager appArtifactManager = addAppArtifact(appArtifactId, ConfigTestApp.class);
    ArtifactId pluginArtifactId = AUTH_NAMESPACE.artifact(pluginArtifactName, pluginArtifactVersion);
    ArtifactManager pluginArtifactManager = addPluginArtifact(pluginArtifactId, appArtifactId, ToStringPlugin.class);
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(appArtifactId, Action.ALL),
        new Privilege(pluginArtifactId, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
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
    Authorizer authorizer = getAuthorizer();
    final ApplicationManager dummyAppManager = deployApplication(AUTH_NAMESPACE.toId(), DummyApp.class);
    ArtifactSummary dummyArtifactSummary = dummyAppManager.getInfo().getArtifact();
    ArtifactId dummyArtifact = AUTH_NAMESPACE.artifact(dummyArtifactSummary.getName(),
                                                       dummyArtifactSummary.getVersion());
    ApplicationId appId = AUTH_NAMESPACE.app(DummyApp.class.getSimpleName());
    final ProgramId serviceId = appId.service(DummyApp.Greeting.SERVICE_NAME);
    DatasetId dsId = AUTH_NAMESPACE.dataset("whom");
    StreamId streamId = AUTH_NAMESPACE.stream("who");
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(appId, Action.ALL),
        new Privilege(serviceId, Action.ALL),
        new Privilege(dsId, Action.ALL),
        new Privilege(streamId, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
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

  @After
  public void cleanupTest() throws Exception {
    Authorizer authorizer = getAuthorizer();
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
      ImmutableSet.of(new Privilege(instance, Action.ADMIN), new Privilege(AUTH_NAMESPACE, Action.ALL)),
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
}
