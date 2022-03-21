/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway;

import com.google.common.collect.ObjectArrays;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.gateway.handlers.PingHandlerTestRun;
import io.cdap.cdap.gateway.handlers.RuntimeArgumentTestRun;
import io.cdap.cdap.gateway.handlers.hooks.MetricsReporterHookTestRun;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.File;
import java.io.IOException;

/**
 * Test Suite for running all API tests.
 */
@RunWith(value = Suite.class)
@Suite.SuiteClasses(value = {
  PingHandlerTestRun.class,
  MetricsReporterHookTestRun.class,
  RuntimeArgumentTestRun.class
})

public class GatewayFastTestsSuite extends GatewayTestBase {

  private static final Header AUTH_HEADER = GatewayTestBase.getAuthHeader();

  public static HttpResponse doGet(String resource) throws Exception {
    return doGet(resource, null);
  }

  public static HttpResponse doGet(String resource, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpGet get = new HttpGet(GatewayTestBase.getEndPoint(resource));
    if (headers != null) {
      get.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {
      get.setHeader(AUTH_HEADER);
    }
    return client.execute(get);
  }

  public static HttpResponse doPut(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(GatewayTestBase.getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPut(String resource, String body) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPut put = new HttpPut(GatewayTestBase.getEndPoint(resource));
    if (body != null) {
      put.setEntity(new StringEntity(body));
    }
    put.setHeader(AUTH_HEADER);
    return client.execute(put);
  }

  public static HttpResponse doPost(HttpPost post) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    post.setHeader(AUTH_HEADER);
    return client.execute(post);
  }

  public static HttpResponse execute(HttpUriRequest request) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    request.setHeader(AUTH_HEADER);
    return client.execute(request);
  }

  public static HttpPost getPost(String resource) throws Exception {
    HttpPost post = new HttpPost(GatewayTestBase.getEndPoint(resource));
    post.setHeader(AUTH_HEADER);
    return post;
  }

  public static HttpPut getPut(String resource) throws Exception {
    HttpPut put = new HttpPut(GatewayTestBase.getEndPoint(resource));
    put.setHeader(AUTH_HEADER);
    return put;
  }

  public static HttpResponse doPost(String resource, String body) throws Exception {
    return doPost(resource, body, null);
  }

  public static HttpResponse doPost(String resource, String body, Header[] headers) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpPost post = new HttpPost(GatewayTestBase.getEndPoint(resource));
    if (body != null) {
      post.setEntity(new StringEntity(body));
    }

    if (headers != null) {
      post.setHeaders(ObjectArrays.concat(AUTH_HEADER, headers));
    } else {
      post.setHeader(AUTH_HEADER);
    }
    return client.execute(post);
  }

  public static HttpResponse doDelete(String resource) throws Exception {
    DefaultHttpClient client = new DefaultHttpClient();
    HttpDelete delete = new HttpDelete(GatewayTestBase.getEndPoint(resource));
    delete.setHeader(AUTH_HEADER);
    return client.execute(delete);
  }

  public static HttpResponse deploy(Class<?> application,
                                    File tmpFolder) throws Exception {

    File artifactJar = buildAppArtifact(application, application.getSimpleName(), tmpFolder);

    HttpEntityEnclosingRequestBase request;
    request = getPost("/v3/namespaces/default/apps");
    request.setHeader(Constants.Gateway.API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name",
                      String.format("%s-1.0.%d.jar", application.getSimpleName(), System.currentTimeMillis()));
    request.setEntity(new FileEntity(artifactJar));
    return execute(request);
  }

  private static File buildAppArtifact(Class<?> cls, String name, File tmpFolder) throws IOException {
    if (!name.endsWith(".jar")) {
      name += ".jar";
    }

    LocationFactory locationFactory = new LocalLocationFactory(tmpFolder);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, cls);
    File destination = new File(DirUtils.createTempDir(tmpFolder), name);
    Locations.linkOrCopy(appJar, destination);
    return destination;
  }

}
