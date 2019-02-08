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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.proto.ApplicationDetail;
import co.cask.cdap.proto.ProgramType;
import co.cask.common.ContentProvider;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link AppFabricServiceMain}.
 */
public class AppFabricServiceMainTest extends MasterServiceMainTestBase {

  @Test
  public void testAppFabricService() throws Exception {

    AppFabricServiceMain main = getServiceMainInstance(AppFabricServiceMain.class);

    // Discovery the app-fabric endpoint
    DiscoveryServiceClient discoveryClient = main.getInjector().getInstance(DiscoveryServiceClient.class);
    Discoverable appFabricEndpoint = new RandomEndpointStrategy(
      () -> discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP)).pick(20, TimeUnit.SECONDS);

    Assert.assertNotNull(appFabricEndpoint);

    // Deploy an app
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location deploymentJar = AppJarHelper.createDeploymentJar(locationFactory, AllProgramsApp.class);

    InetSocketAddress addr = appFabricEndpoint.getSocketAddress();
    URI baseURI = URI.create(String.format("http://%s:%d/v3/namespaces/default/",
                                           addr.getHostName(), addr.getPort()));

    URL url = baseURI.resolve("apps").toURL();
    HttpRequestConfig requestConfig = new HttpRequestConfig(0, 0);
    HttpResponse response = HttpRequests.execute(
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
}
