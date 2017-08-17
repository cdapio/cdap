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
import co.cask.cdap.api.Config;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ProgramResources;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpResponse;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.Callable;
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
  private static File wordCountAppJar;

  @BeforeClass
  public static void setup() throws Exception {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    wordCountAppJar = buildAppArtifact(WordCountApp.class, WORDCOUNT_APP_NAME);
  }

  // test that the call to deploy an artifact and application in a single step will delete the artifact
  // if the application could not be created
  @Test(expected = ArtifactNotFoundException.class)
  public void testDeployArtifactAndApplicationCleansUpArtifactOnFailure() throws Exception {
    NamespaceId.DEFAULT.artifact("missing-mr", "1.0.0-SNAPSHOT");
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "missing-mr", "1.0.0-SNAPSHOT");
    File appJarFile = buildAppArtifact(MissingMapReduceWorkflowApp.class, "missing-mr");

    try {
      applicationLifecycleService.deployAppAndArtifact(NamespaceId.DEFAULT, "appName", artifactId, appJarFile, null,
                                                       null, new ProgramTerminator() {
          @Override
          public void stop(ProgramId programId) throws Exception {
            // no-op
          }
        }, true);
      Assert.fail("expected application deployment to fail.");
    } catch (Exception e) {
      // expected
    }

    // the artifact should have been cleaned up, and this should throw a not found exception
    artifactRepository.getArtifact(artifactId);
  }

  @Test
  public void testAppDeletionMultipleNS() throws Exception {
    artifactRepository.addArtifact(
      NamespaceId.DEFAULT.artifact(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT").toId(), wordCountAppJar);
    artifactRepository.addArtifact(
      TEST_NAMESPACE_META1.getNamespaceId().artifact(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT").toId(), wordCountAppJar);

    // deploy same app in two namespaces
    HttpResponse response = deploy(
      TEST_NAMESPACE_META1.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT")));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    response = deploy(
      NamespaceId.DEFAULT.app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT")));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    ProgramId wordcountFlow1 = new ProgramId(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW,
                                             WORDCOUNT_FLOW_NAME);
    ApplicationId defaultNSAppId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), WORDCOUNT_APP_NAME);
    ApplicationId testNSAppId = new ApplicationId(TEST_NAMESPACE1, WORDCOUNT_APP_NAME);

    // start a program in one namespace
    // flow is stopped initially
    Assert.assertEquals("STOPPED", getProgramStatus(wordcountFlow1.toId()));
    // start a flow and check the status
    startProgram(wordcountFlow1.toId());
    waitState(wordcountFlow1.toId(), ProgramRunStatus.RUNNING.toString());

    // delete the app from another (default namespace)
    deleteApp(defaultNSAppId.toId(), 200);

    // cleanup
    stopProgram(wordcountFlow1.toId());
    waitState(wordcountFlow1.toId(), "STOPPED");
    deleteApp(testNSAppId.toId(), 200);
  }

  /**
   * Some tests for owner information storage/propagation during app deployment.
   * More tests at handler level {@link co.cask.cdap.internal.app.services.http.handlers.AppLifecycleHttpHandlerTest}
   */
  @Test
  public void testOwner() throws Exception {
    // Create a namespace and deploy the artifact
    NamespaceMeta namespaceMeta =
      new NamespaceMeta.Builder().setName("testOwner").build();
    createNamespace(GSON.toJson(namespaceMeta), namespaceMeta.getName());

    artifactRepository.addArtifact(
      namespaceMeta.getNamespaceId().artifact(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT").toId(), wordCountAppJar);

    // Deploy the app with a owner
    String ownerPrincipal = "alice/somehost.net@somekdc.net";
    HttpResponse response = deploy(
      namespaceMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT"), null, ownerPrincipal));

    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    // trying to redeploy the same app as a different owner should fail
    response = deploy(
      namespaceMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT"), null,
                             "bob/somehost.net@somekdc.net"));
    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.getCode(),
                        response.getStatusLine().getStatusCode());

    // although trying to re-deploy the app with same owner should work
    response = deploy(
      namespaceMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT"), null, ownerPrincipal));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());
  }

  @Test
  public void testOwnerInImpersonatedNS() throws Exception {
    // create a namespace with principal
    String nsPrincipal = "nsCreator/somehost.net@somekdc.net";
    String nsKeytabURI = "some/path";
    NamespaceMeta impNsMeta =
      new NamespaceMeta.Builder().setName("impNs").setPrincipal(nsPrincipal).setKeytabURI(nsKeytabURI).build();
    createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());

    // Deploy the artifact to the namespace first
    artifactRepository.addArtifact(
      impNsMeta.getNamespaceId().artifact(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT").toId(), wordCountAppJar);

    // deploy an app without owner
    HttpResponse response = deploy(
      impNsMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT")));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    // try to redeploy with some owner different than namespace principal it should fail
    response = deploy(
      impNsMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT"), null,
                             "bob/somehost.net@somekdc.net"));
    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.getCode(), response.getStatusLine().getStatusCode());

    // although trying to re-deploy the app with namespace principal should work
    response = deploy(
      impNsMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT"), null, nsPrincipal));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    // re-deploy without any owner should also work
    response = deploy(
      impNsMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT")));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(),
                        response.getStatusLine().getStatusCode());

    // cleanup
    deleteNamespace(impNsMeta.getName());
    Assert.assertEquals(HttpResponseCodes.SC_NOT_FOUND,
                        getNamespace(impNsMeta.getName()).getStatusLine().getStatusCode());

    // create an impersonated ns again
    createNamespace(GSON.toJson(impNsMeta), impNsMeta.getName());

    // Redeploy the artifact to the namespace again
    artifactRepository.addArtifact(
      impNsMeta.getNamespaceId().artifact(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT").toId(), wordCountAppJar);

    // deploy an app with an owner
    response = deploy(
      impNsMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT"), null,
                             "bob/somehost.net@somekdc.net"));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatusLine().getStatusCode());

    // re-deploy with namespace principal should fail
    response = deploy(
      impNsMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT"), null,
                             nsPrincipal));
    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.getCode(),
                        response.getStatusLine().getStatusCode());

    // although redeploy with same app principal should work
    response = deploy(
      impNsMeta.getNamespaceId().app(WORDCOUNT_APP_NAME),
      new AppRequest<Config>(new ArtifactSummary(WORDCOUNT_APP_NAME, "1.0.0-SNAPSHOT"), null,
                             "bob/somehost.net@somekdc.net"));
    Assert.assertEquals(HttpResponseStatus.OK.getCode(),
                        response.getStatusLine().getStatusCode());
  }

  @Test
  public void testMissingDependency() throws Exception {
    // tests the fix for CDAP-2543, by having programs which fail to start up due to missing dependency jars
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("missing-guava-dependency", "1.0.0-SNAPSHOT");
    Location appJar = createDeploymentJar(locationFactory, AppWithProgramsUsingGuava.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getArtifact(), artifactId.getVersion()));
    Locations.linkOrCopy(appJar, appJarFile);
    appJar.delete();

    applicationLifecycleService.deployAppAndArtifact(NamespaceId.DEFAULT, "appName", artifactId.toId(), appJarFile,
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
    startProgram(worker.toId());
    waitForRuns(1, worker, ProgramRunStatus.FAILED);

    // fail the MapReduce#initialize
    ProgramId mapreduce = appId.mr(AppWithProgramsUsingGuava.NoOpMR.NAME);
    startProgram(mapreduce.toId());
    waitForRuns(1, mapreduce, ProgramRunStatus.FAILED);

    // fail the CustomAction#initialize
    ProgramId workflow = appId.workflow(AppWithProgramsUsingGuava.NoOpWorkflow.NAME);
    startProgram(workflow.toId());
    waitForRuns(1, workflow, ProgramRunStatus.FAILED);

    // fail the Workflow#initialize
    appId.workflow(AppWithProgramsUsingGuava.NoOpWorkflow.NAME);
    startProgram(workflow.toId(), ImmutableMap.of("fail.in.workflow.initialize", "true"));
    waitForRuns(1, workflow, ProgramRunStatus.FAILED);
  }

  private void waitForRuns(int expected, final ProgramId programId, final ProgramRunStatus status) throws Exception {
    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getProgramRuns(programId.toId(), status.toString()).size();
      }
    }, 5, TimeUnit.SECONDS);
  }

  // creates an application jar for the given application class, but excludes classes that begin with 'com.google.'
  // similar to AppJarHelper#createDeploymentJar
  private static Location createDeploymentJar(LocationFactory locationFactory, Class<?> clz,
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
}
