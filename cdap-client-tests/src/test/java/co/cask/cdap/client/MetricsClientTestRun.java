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

package co.cask.cdap.client;

import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.PingService;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsTags;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.MetricTagValue;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.cdap.test.XSlowTests;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link MetricsClient}.
 */
@Category(XSlowTests.class)
public class MetricsClientTestRun extends ClientTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsClientTestRun.class);

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

      long curTime = System.currentTimeMillis();
      HttpResponse response = HttpRequests.execute(HttpRequest.get(pingURL).build());
      Assert.assertEquals(200, response.getResponseCode());
      long duration = System.currentTimeMillis() - curTime;
      LOG.info("Took {} milliseonds to send the query", duration);

      curTime = System.currentTimeMillis();
      Tasks.waitFor(true, () ->
        metricsClient.query(MetricsTags.service(service), Constants.Metrics.Name.Service.SERVICE_INPUT)
          .getSeries().length > 0, 10, TimeUnit.SECONDS);
      duration = System.currentTimeMillis() - curTime;
      LOG.info("Took {} milliseonds for the first metrics to be available", duration);

      curTime = System.currentTimeMillis();
      MetricQueryResult result =
        metricsClient.query(MetricsTags.service(service),
                            Collections.singletonList(Constants.Metrics.Name.Service.SERVICE_INPUT),
                            Collections.emptyList(), ImmutableMap.of("start", "now-20s", "end", "now"));
      duration = System.currentTimeMillis() - curTime;
      LOG.info("Took {} milliseonds to complete the query", duration);
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      curTime = System.currentTimeMillis();
      result = metricsClient.query(MetricsTags.service(service), Constants.Metrics.Name.Service.SERVICE_INPUT);
      duration = System.currentTimeMillis() - curTime;
      LOG.info("Took {} milliseonds to complete the query", duration);
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      curTime = System.currentTimeMillis();
      result = metricsClient.query(MetricsTags.service(service),
                                   Collections.singletonList(Constants.Metrics.Name.Service.SERVICE_INPUT),
                                   Collections.emptyList(), Collections.singletonMap("aggregate", "true"));
      duration = System.currentTimeMillis() - curTime;
      LOG.info("Took {} milliseonds to complete the query", duration);
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
