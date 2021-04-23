/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.deploy.pipeline.ConfiguratorConfig;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;


/**
 * Unit test for {@link TaskWorkerService}.
 */
public class TaskWorkerServiceTest {
  private static final Gson GSON = new Gson();

  private CConfiguration createCConf() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setStrings(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setLong(Constants.Preview.REQUEST_POLL_DELAY_MILLIS, 200);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    return cConf;
  }

  private SConfiguration createSConf() {
    SConfiguration sConf = SConfiguration.create();
    return sConf;
  }

  private Configuration createHConf() {
    Configuration hConf = new Configuration();
    return hConf;
  }

  @Test
  public void testGSON() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set("test", "test");
    ConfiguratorConfig configuratorConfig = new ConfiguratorConfig(
      cConf, Id.Namespace.from("TEST"),
      new Id.Artifact(Id.Namespace.from("art"), "TTT", new ArtifactVersion("1.0.0")),
      "test",
      "appNane", "1.0.0",
      "long config", new LocalLocationFactory().getHomeLocation().toURI());
    GSON.toJson(configuratorConfig);
    Assert.assertEquals(configuratorConfig.getcConf().get("test"), "test");
  }

  @Test
  public void testStartAndStop() throws IOException {
    CConfiguration cConf = createCConf();
    SConfiguration sConf = createSConf();
    Configuration hConf = createHConf();

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    TaskWorkerService taskWorkerService = new TaskWorkerService(cConf, sConf, hConf, discoveryService);

    // start the service
    taskWorkerService.startAndWait();

    InetSocketAddress addr = taskWorkerService.getBindAddress();
    URI uri = URI.create(String.format("http://%s:%s", addr.getHostName(), addr.getPort()));

    HttpResponse response;

    // Get request
    response = HttpRequests.execute(HttpRequest.get(uri.resolve("/v3Internal/worker/get").toURL()).build(),
                                    new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());


    // Post valid request
    String want = "test-input";
    RunnableTaskRequest req = new RunnableTaskRequest(EchoRunnableTask.class.getName(), want);
    String reqBody = GSON.toJson(req);
    response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(want, response.getResponseBodyAsString());

    // Post bad request
    RunnableTaskRequest noClassReq = new RunnableTaskRequest("NoClass", "");
    reqBody = GSON.toJson(noClassReq);
    response = HttpRequests.execute(
      HttpRequest.post(uri.resolve("/v3Internal/worker/run").toURL())
        .withBody(reqBody).build(),
      new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, response.getResponseCode());
    Assert.assertTrue(response.getResponseBodyAsString().contains("java.lang.ClassNotFoundException"));

    // stop the service
    taskWorkerService.stopAndWait();
  }
}
