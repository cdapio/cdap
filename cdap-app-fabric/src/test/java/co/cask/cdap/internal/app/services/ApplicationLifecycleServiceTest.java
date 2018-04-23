/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.AppWithProgramsUsingGuava;
import co.cask.cdap.MissingMapReduceWorkflowApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ProgramResources;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

/**
 */
public class ApplicationLifecycleServiceTest extends AppFabricTestBase {

  private static final String WORDCOUNT_APP_NAME = "WordCountApp";
  private static final String WORDCOUNT_FLOW_NAME = "WordCountFlow";

  private static ApplicationLifecycleService applicationLifecycleService;
  private static LocationFactory locationFactory;
  private static ArtifactRepository artifactRepository;

  @BeforeClass
  public static void setup() throws Exception {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
  }

  // test that the call to deploy an artifact and application in a single step will delete the artifact
  // if the application could not be created
  @Test(expected = ArtifactNotFoundException.class)
  public void testDeployArtifactAndApplicationCleansUpArtifactOnFailure() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "missing-mr", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, MissingMapReduceWorkflowApp.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();

    try {
      applicationLifecycleService.deployAppAndArtifact(NamespaceId.DEFAULT, "appName", artifactId, appJarFile, null,
                                                       null, programId -> { }, true);
      Assert.fail("expected application deployment to fail.");
    } catch (Exception e) {
      // expected
    }

    // the artifact should have been cleaned up, and this should throw a not found exception
    artifactRepository.getArtifact(artifactId);
  }

  @Test
  public void testAppDeletionMultipleNS() throws Exception {

    // deploy same app in two namespaces
    deploy(WordCountApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    deploy(WordCountApp.class, 200);
    ProgramId wordcountFlow1 = new ProgramId(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW,
                                             WORDCOUNT_FLOW_NAME);
    ApplicationId defaultNSAppId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), WORDCOUNT_APP_NAME);
    ApplicationId testNSAppId = new ApplicationId(TEST_NAMESPACE1, WORDCOUNT_APP_NAME);

    // start a program in one namespace
    // flow is stopped initially
    Assert.assertEquals("STOPPED", getProgramStatus(Id.Program.fromEntityId(wordcountFlow1)));
    // start a flow and check the status
    startProgram(Id.Program.fromEntityId(wordcountFlow1));
    waitState(wordcountFlow1, ProgramRunStatus.RUNNING.toString());

    // delete the app from another (default namespace)
    deleteAppAndData(defaultNSAppId);

    // cleanup
    stopProgram(Id.Program.fromEntityId(wordcountFlow1));
    waitState(wordcountFlow1, "STOPPED");
    deleteAppAndData(testNSAppId);
  }

  /**
   * Some tests for owner information storage/propagation during app deployment.
   * More tests at handler level {@link co.cask.cdap.internal.app.services.http.handlers.AppLifecycleHttpHandlerTest}
   */
  @Test
  public void testOwner() throws Exception {
    String ownerPrincipal = "alice/somehost.net@somekdc.net";
    deploy(WordCountApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           TEST_NAMESPACE1, ownerPrincipal);

    // trying to redeploy the same app as a different owner should fail
    deploy(WordCountApp.class, HttpResponseStatus.FORBIDDEN.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           TEST_NAMESPACE1, "bob/somehost.net@somekdc.net");

    // although trying to re-deploy the app with same owner should work
    deploy(WordCountApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           TEST_NAMESPACE1, ownerPrincipal);

    // delete, otherwise it conflicts with other tests
    deleteAppAndData(new NamespaceId(TEST_NAMESPACE1).app(WordCountApp.NAME));
  }

  @Test
  public void testOwnerInImpersonatedNS() throws Exception {
    // create a namespace with principal
    String nsPrincipal = "nsCreator/somehost.net@somekdc.net";
    String nsKeytabURI = "some/path";
    NamespaceMeta impNsMeta =
      new NamespaceMeta.Builder().setName("impNs").setPrincipal(nsPrincipal).setKeytabURI(nsKeytabURI).build();
    createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());

    // deploy an app without owner
    deploy(WordCountApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName());

    // try to redeploy with some owner different than namespace principal it should fail
    deploy(WordCountApp.class, HttpResponseStatus.FORBIDDEN.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), "bob/somehost.net@somekdc.net");

    // although trying to re-deploy the app with namespace principal should work
    deploy(WordCountApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), nsPrincipal);

    // re-deploy without any owner should also work
    deploy(WordCountApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName());

    // cleanup
    deleteNamespace(impNsMeta.getName());
    Assert.assertEquals(HttpResponseCodes.SC_NOT_FOUND,
                        getNamespace(impNsMeta.getName()).getStatusLine().getStatusCode());

    // create an impersonated ns again
    createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());

    // deploy an app with an owner
    deploy(WordCountApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), "bob/somehost.net@somekdc.net");

    // re-deploy with namespace principal should fail
    deploy(WordCountApp.class, HttpResponseStatus.FORBIDDEN.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), impNsMeta.getConfig().getPrincipal());

    // although redeploy with same app principal should work
    deploy(WordCountApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           impNsMeta.getName(), "bob/somehost.net@somekdc.net");

    // delete, otherwise it conflicts with other tests
    deleteAppAndData(impNsMeta.getNamespaceId().app(WordCountApp.NAME));
  }

  @Test
  public void testMissingDependency() throws Exception {
    // tests the fix for CDAP-2543, by having programs which fail to start up due to missing dependency jars
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("missing-guava-dependency", "1.0.0-SNAPSHOT");
    Location appJar = createDeploymentJar(locationFactory, AppWithProgramsUsingGuava.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();

    applicationLifecycleService.deployAppAndArtifact(NamespaceId.DEFAULT, "appName",
                                                     Id.Artifact.fromEntityId(artifactId), appJarFile,
                                                     null, null, new ProgramTerminator() {
      @Override
      public void stop(ProgramId programId) throws Exception {
        // no-op
      }
    }, true);

    ApplicationId appId = NamespaceId.DEFAULT.app("appName");

    // run records for programs that have missing dependencies should be FAILED, instead of hanging in RUNNING

    // fail the Worker#initialize
    ProgramId worker = appId.worker(AppWithProgramsUsingGuava.NoOpWorker.NAME);
    startProgram(Id.Program.fromEntityId(worker));
    waitForRuns(1, worker, ProgramRunStatus.FAILED);

    // fail the MapReduce#initialize
    ProgramId mapreduce = appId.mr(AppWithProgramsUsingGuava.NoOpMR.NAME);
    startProgram(Id.Program.fromEntityId(mapreduce));
    waitForRuns(1, mapreduce, ProgramRunStatus.FAILED);

    // fail the CustomAction#initialize
    ProgramId workflow = appId.workflow(AppWithProgramsUsingGuava.NoOpWorkflow.NAME);
    startProgram(Id.Program.fromEntityId(workflow));
    waitForRuns(1, workflow, ProgramRunStatus.FAILED);

    // fail the Workflow#initialize
    appId.workflow(AppWithProgramsUsingGuava.NoOpWorkflow.NAME);
    startProgram(Id.Program.fromEntityId(workflow), ImmutableMap.of("fail.in.workflow.initialize", "true"));
    waitForRuns(1, workflow, ProgramRunStatus.FAILED);
  }

  private void waitForRuns(int expected, final ProgramId programId, final ProgramRunStatus status) throws Exception {
    Tasks.waitFor(expected, () -> getProgramRuns(Id.Program.fromEntityId(programId), status).size(),
                  5, TimeUnit.SECONDS);
  }

  // creates an application jar for the given application class, but excludes classes that begin with 'com.google.'
  // similar to AppJarHelper#createDeploymentJar
  public static Location createDeploymentJar(LocationFactory locationFactory, Class<?> clz,
                                             File... bundleEmbeddedJars) throws IOException {
    return AppJarHelper.createDeploymentJar(locationFactory, clz, new Manifest(), new ClassAcceptor() {
      final Set<String> visibleResources = ProgramResources.getVisibleResources();

      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        if (visibleResources.contains(className.replace('.', '/') + ".class")) {
          return false;
        }
        // TODO: Fix it with CDAP-5800
        if (className.startsWith("org.apache.spark.")) {
          return false;
        }
        // exclude a necessary dependency
        if (className.startsWith("com.google.")) {
          return false;
        }
        return true;
      }
    }, bundleEmbeddedJars);
  }

  private void deleteAppAndData(ApplicationId applicationId) throws Exception {
    deleteApp(applicationId, 200);
    deleteNamespaceData(applicationId.getNamespace());
    deleteStream(applicationId.getNamespaceId().stream(WordCountApp.STREAM_NAME));
  }
}
