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

import com.google.gson.Gson;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.common.ContentProvider;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

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
}
