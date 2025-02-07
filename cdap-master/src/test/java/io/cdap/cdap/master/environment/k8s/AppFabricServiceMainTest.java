/*
 * Copyright © 2019-2023 Cask Data, Inc.
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
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link AppFabricServiceMain}.
 */
public class AppFabricServiceMainTest extends MasterServiceMainTestBase {

  @Test
  public void testAppFabricService() throws Exception {

    // Query the system services endpoint
    URL url = getRouterBaseUri().resolve("/v3/system/services").toURL();
    HttpResponse response = HttpRequests
        .execute(HttpRequest.get(url).build(), new DefaultHttpRequestConfig(false));

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Create a namespace.
    final String name = "test_namespace";
    final String description = "test namespace description";
    URI baseUri = getRouterBaseUri().resolve("/v3/namespaces/" + name);
    url = baseUri.toURL();
    String requestBody =
        String.format("{\"name\":\"%s\", \"description\":\"%s\"}", name, description);
    HttpRequestConfig requestConfig = new HttpRequestConfig(0, 0, false);
    response = HttpRequests.execute(
        HttpRequest
            .put(url)
            .withBody(requestBody)
            .build(), requestConfig);

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    // Get the namespace.
    response = HttpRequests.execute(HttpRequest.get(url).build(), requestConfig);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    NamespaceMeta namespaceMeta = new Gson()
        .fromJson(response.getResponseBodyAsString(), NamespaceMeta.class);

    // Do some basic validation only.
    Assert.assertEquals(name, namespaceMeta.getName());
    Assert.assertEquals(description, namespaceMeta.getDescription());
  }
}
