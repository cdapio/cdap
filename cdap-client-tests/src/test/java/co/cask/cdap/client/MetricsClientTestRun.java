/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.client.app.FakeFlow;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsTags;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.MetricTagValue;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.FlowletId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
  private StreamClient streamClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    programClient = new ProgramClient(clientConfig);
    streamClient = new StreamClient(clientConfig);
    metricsClient = new MetricsClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    appClient.deploy(NamespaceId.DEFAULT, createAppJarFile(FakeApp.class));

    ApplicationId app = NamespaceId.DEFAULT.app(FakeApp.NAME);
    ProgramId flow = app.flow(FakeFlow.NAME);
    StreamId stream = NamespaceId.DEFAULT.stream(FakeApp.STREAM_NAME);

    try {
      programClient.start(flow);
      streamClient.sendEvent(stream, "hello world");

      // TODO: remove arbitrary sleep
      TimeUnit.SECONDS.sleep(5);

      FlowletId flowletId = flow.flowlet(FakeFlow.FLOWLET_NAME);

      MetricQueryResult result = metricsClient.query(MetricsTags.flowlet(flowletId),
                                                     Constants.Metrics.Name.Flow.FLOWLET_INPUT);
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      result = metricsClient.query(MetricsTags.flowlet(flowletId),
                                   ImmutableList.of(Constants.Metrics.Name.Flow.FLOWLET_INPUT),
                                   ImmutableList.<String>of(), ImmutableMap.of("aggregate", "true"));
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      result = metricsClient.query(MetricsTags.flowlet(flowletId),
                                   ImmutableList.of(Constants.Metrics.Name.Flow.FLOWLET_INPUT),
                                   ImmutableList.<String>of(), ImmutableMap.of("start", "now-20s", "end", "now"));
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      List<MetricTagValue> tags = metricsClient.searchTags(MetricsTags.flowlet(flowletId));
      Assert.assertEquals(1, tags.size());
      Assert.assertEquals("run", tags.get(0).getName());

      List<String> metrics = metricsClient.searchMetrics(MetricsTags.flowlet(flowletId));
      Assert.assertTrue(metrics.contains(Constants.Metrics.Name.Flow.FLOWLET_INPUT));
    } finally {
      programClient.stop(flow);
      assertProgramRuns(programClient, flow, ProgramRunStatus.KILLED, 1, 10);
      appClient.delete(app);
    }
  }
}
