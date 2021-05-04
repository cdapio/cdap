/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import com.google.common.io.Files;
import io.cdap.cdap.internal.app.worker.RunnableTask;
import io.cdap.cdap.internal.app.worker.RunnableTaskRequest;
import io.cdap.cdap.internal.app.worker.TaskWorkerService;
import io.cdap.cdap.internal.app.worker.TaskWorkerTwillRunnable;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ServiceListenerAdapter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.CompletableFuture;

/**
 * Unit test for {@link AppFabricServiceMain}.
 */
public class AppFabricServiceMainTest extends MasterServiceMainTestBase {

  @Test
  public void testAppFabricService() throws Exception {

    // Query the system services endpoint
    URL url = getRouterBaseURI().resolve("/v3/system/services").toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build(), new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Deploy an app
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location deploymentJar = AppJarHelper.createDeploymentJar(locationFactory, AllProgramsApp.class);

    URI baseURI = getRouterBaseURI().resolve("/v3/namespaces/default/");
    url = baseURI.resolve("apps").toURL();
    HttpRequestConfig requestConfig = new HttpRequestConfig(0, 0, false);
    response = HttpRequests.execute(
      HttpRequest
        .post(url)
        .withBody((ContentProvider<? extends InputStream>) deploymentJar::getInputStream)
        .addHeader("X-Archive-Name", AllProgramsApp.class.getSimpleName() + "-1.0-SNAPSHOT.jar")
        .build(), requestConfig);

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Get the application
    url = baseURI.resolve("apps/" + AllProgramsApp.NAME).toURL();
    response = HttpRequests.execute(HttpRequest.get(url).build(), requestConfig);

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    ApplicationDetail appDetail = new Gson().fromJson(response.getResponseBodyAsString(), ApplicationDetail.class);

    // Do some basic validation only.
    Assert.assertEquals(AllProgramsApp.NAME, appDetail.getName());
    Assert.assertTrue(appDetail.getPrograms()
      .stream()
      .filter(r -> r.getType() == ProgramType.WORKFLOW)
      .anyMatch(r -> AllProgramsApp.NoOpWorkflow.NAME.equals(r.getName())));
  }


  private CConfiguration createCConf(int port) {
    CConfiguration cConf = this.cConf;
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, port);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    return cConf;
  }

  private SConfiguration createSConf() {
    SConfiguration sConf = this.sConf;
    return sConf;
  }

  private TaskWorkerService setupTaskWorkerService(int port) {
    CConfiguration cConf = createCConf(port);
    SConfiguration sConf = createSConf();
    org.apache.hadoop.conf.Configuration hConf = new org.apache.hadoop.conf.Configuration();


    DiscoveryService discoveryService = getInjector().getInstance(DiscoveryService.class);
    ArtifactRepositoryReader artifactRepositoryReader = getInjector().getInstance(RemoteArtifactRepositoryReader.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    Impersonator impersonator = getInjector().getInstance(Impersonator.class);

    TaskWorkerService taskWorkerService = new TaskWorkerService(
      cConf, sConf, discoveryService, artifactRepositoryReader, artifactRepository, impersonator);

    // start the service
    taskWorkerService.startAndWait();
    return taskWorkerService;
  }

  private void waitForTaskWorkerToFinish(TaskWorkerService taskWorker) {
    CompletableFuture<Service.State> future = new CompletableFuture<>();
    taskWorker.addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(Service.State from) {
        future.complete(from);
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        future.completeExceptionally(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    try {
      Uninterruptibles.getUninterruptibly(future);
    } catch (Exception e) {
    }
  }

  public static class TestRunnableClass extends RunnableTask {
    @Override
    protected byte[] run(String param) throws Exception {
      if (!param.equals("")) {
        Thread.sleep(Integer.valueOf(param));
      }
      return param.getBytes();
    }

    @Override
    protected void startUp() throws Exception {

    }

    @Override
    protected void shutDown() throws Exception {

    }
  }
  private Injector getInjector() {
    return getServiceMainInstance(AppFabricServiceMain.class).getInjector();
  }

  @Test
  public void testStartAndStopWithValidRequest() throws Exception {
    LocationFactory locationFactory = getInjector().getInstance(LocationFactory.class);
    ArtifactRepository artifactRepository = getInjector().getInstance(ArtifactRepository.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "some-task", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, TestRunnableClass.class);
    File appJarFile = new File(TEMP_FOLDER.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();

    artifactRepository.addArtifact(artifactId, appJarFile);
    ArtifactSummary summary = new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion(),
                                                  ArtifactScope.SYSTEM);
    System.out.println("wyzhang: " + summary.toString());
    System.out.println("wyzhang: " + artifactId.toString());

    Gson GSON = new Gson();
    TaskWorkerService taskWorkerService = setupTaskWorkerService(10001);
    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    // Post valid request
    String want = "100";
    RunnableTaskRequest req = new RunnableTaskRequest(artifactId, TestRunnableClass.class.getName(), want);
    String reqBody = GSON.toJson(req);
    HttpResponse response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    waitForTaskWorkerToFinish(taskWorkerService);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(want, response.getResponseBodyAsString());
    Assert.assertTrue(taskWorkerService.state() == Service.State.TERMINATED);
  }
}
