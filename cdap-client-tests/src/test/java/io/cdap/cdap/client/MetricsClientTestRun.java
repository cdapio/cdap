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

package io.cdap.cdap.client;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.client.app.FakeApp;
import io.cdap.cdap.client.app.PingService;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.metrics.MetricsTags;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.MetricQueryResult;
import io.cdap.cdap.proto.MetricTagValue;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link MetricsClient}.
 */
@Category(XSlowTests.class)
public class MetricsClientTestRun extends ClientTestBase {

  private MetricsClient metricsClient;
  private ApplicationClient appClient;
  private ProgramClient programClient;
  private ServiceClient serviceClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
    serviceClient = new ServiceClient(clientConfig);
    metricsClient = new MetricsClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(FakeApp.class));

    ApplicationId app = NamespaceId.DEFAULT.app(FakeApp.NAME);
    ServiceId service = app.service(PingService.NAME);

    try {
      programClient.start(service);
      programClient.waitForStatus(service, ProgramStatus.RUNNING, 15, TimeUnit.SECONDS);

      URL serviceURL = serviceClient.getServiceURL(service);
      URL pingURL = new URL(serviceURL, "ping");

      HttpResponse response = HttpRequests.execute(HttpRequest.get(pingURL).build(),
                                                   new DefaultHttpRequestConfig(false));
      Assert.assertEquals(200, response.getResponseCode());

      Tasks.waitFor(true, () ->
        metricsClient.query(MetricsTags.service(service),
                            Collections.singletonList(Constants.Metrics.Name.Service.SERVICE_INPUT),
                            Collections.emptyList(), ImmutableMap.of("start", "now-20s", "end", "now"))
          .getSeries().length > 0, 10, TimeUnit.SECONDS);

      MetricQueryResult result = metricsClient.query(MetricsTags.service(service),
                                                     Constants.Metrics.Name.Service.SERVICE_INPUT);
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      result = metricsClient.query(MetricsTags.service(service),
                                   Collections.singletonList(Constants.Metrics.Name.Service.SERVICE_INPUT),
                                   Collections.emptyList(), Collections.singletonMap("aggregate", "true"));
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      result = metricsClient.query(MetricsTags.service(service),
                                   Collections.singletonList(Constants.Metrics.Name.Service.SERVICE_INPUT),
                                   Collections.emptyList(), ImmutableMap.of("start", "now-20s", "end", "now"));
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      List<MetricTagValue> tags = metricsClient.searchTags(MetricsTags.service(service));
      Assert.assertEquals(1, tags.size());
      Assert.assertEquals("run", tags.get(0).getName());

      List<String> metrics = metricsClient.searchMetrics(MetricsTags.service(service));
      Assert.assertTrue(metrics.contains(Constants.Metrics.Name.Service.SERVICE_INPUT));
    } finally {
      programClient.stop(service);
      assertProgramRuns(programClient, service, ProgramRunStatus.KILLED, 1, 10);
      appClient.delete(app);
    }
  }
}
