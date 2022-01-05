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
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.services.SupportBundleService;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
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
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link SupportBundleServiceMain}.
 */
public class SupportBundleServiceMainTest extends MasterServiceMainTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleServiceMainTest.class);

  @Test
  public void testSupportBundleService() throws Exception {
    Injector injector = getServiceMainInstance(SupportBundleServiceMain.class).getInjector();
    URL url = getRouterBaseURI().resolve("/v3/support/bundle").toURL();
    HttpResponse response = HttpRequests.execute(HttpRequest.post(url).build(),
                                                 new DefaultHttpRequestConfig(false));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());

    SupportBundleService supportBundleService = injector.getInstance(
        SupportBundleService.class);
    SupportBundleConfiguration supportBundleConfiguration = new SupportBundleConfiguration(
      NamespaceId.DEFAULT.getNamespace(), null, null, ProgramType.WORKFLOW
        , "DataPipelineWorkflow", 1);
    String uuid = supportBundleService.generateSupportBundle(supportBundleConfiguration);
    Assert.assertNotNull(uuid);
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
