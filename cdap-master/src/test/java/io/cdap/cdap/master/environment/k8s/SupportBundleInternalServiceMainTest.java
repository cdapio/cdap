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

import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.SupportBundle;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit test for {@link SupportBundleServiceMain}.
 */
public class SupportBundleInternalServiceMainTest extends MasterServiceMainTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      SupportBundleInternalServiceMainTest.class);

  @Test
  public void testSupportBundleService()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    Injector injector = getServiceMainInstance(SupportBundleServiceMain.class).getInjector();

    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(
        DiscoveryServiceClient.class);
    // Query the appended logs, we can not query logs for a given run because run record does not exist
//    String url = "/v3/support/bundle";
//    Tasks.waitFor(true, () -> {
//      HttpResponse response = doPost(url, discoveryServiceClient, Service.SUPPORT_BUNDLE_SERVICE);
//      if (response.getResponseCode() != 200) {
//        LOG.warn("testLogsService get logs response non 200 response code: {} {} {}",
//                 response.getResponseCode(), response.getResponseMessage(),
//                 response.getResponseBodyAsString());
//        return false;
//      }
//      String uuid = response.getResponseBodyAsString();
//      return uuid.length() > 0;
//    }, 5, TimeUnit.MINUTES, 500, TimeUnit.MILLISECONDS);
    URL url = getRouterBaseURI().resolve("/v3/support/bundle").toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.post(url).build(),
                                                 new DefaultHttpRequestConfig(false));
//
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

//    SupportBundleService supportBundleService = injector.getInstance(
//        SupportBundleService.class);
//    SupportBundleConfiguration supportBundleConfiguration = new SupportBundleConfiguration(
//        NamespaceId.SYSTEM.getNamespace(), null, null, SupportBundle.DEFAULT_WORKFLOW
//        , 1);
//    String uuid = supportBundleService.generateSupportBundle(supportBundleConfiguration);
//    Assert.assertNotNull(uuid);
  }

  private HttpResponse doPost(String path, DiscoveryServiceClient discoveryServiceClient,
                              String serviceName) throws IOException {
    Discoverable discoverable = new RandomEndpointStrategy(
        () -> discoveryServiceClient.discover(serviceName)).pick(10, TimeUnit.SECONDS);
    Assert.assertNotNull(discoverable);
    URL url = URIScheme.createURI(discoverable, path).toURL();
    return HttpRequests.execute(HttpRequest.post(url).build(), new DefaultHttpRequestConfig(false));
  }
}
