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
import co.cask.cdap.common.metrics.MetricsConstants;
import co.cask.cdap.common.metrics.MetricsContexts;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetricQueryResult;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
    appClient.deploy(createAppJarFile(FakeApp.class));

    try {
      programClient.start(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
      streamClient.sendEvent(FakeApp.STREAM_NAME, "hello world");

      // TODO: remove arbitrary sleep
      TimeUnit.SECONDS.sleep(5);

      Id.Application appId = Id.Application.from(Constants.DEFAULT_NAMESPACE_ID, FakeApp.NAME);
      Id.Program programId = Id.Program.from(appId, ProgramType.FLOW, FakeFlow.NAME);
      String flowlet = FakeFlow.FLOWLET_NAME;

      MetricQueryResult result = metricsClient.query(MetricsContexts.forFlowlet(programId, flowlet),
                                                     MetricsConstants.FLOWLET_INPUT, null);
      Assert.assertEquals(1, result.getSeries()[0].getData()[0].getValue());

      // TODO: more tests
    } finally {
      programClient.stop(FakeApp.NAME, ProgramType.FLOW, FakeFlow.NAME);
      appClient.delete(FakeApp.NAME);
    }
  }
}
