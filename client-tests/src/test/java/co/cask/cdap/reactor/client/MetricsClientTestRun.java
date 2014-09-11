/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client;

import co.cask.cdap.client.MetricsClient;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for {@link MetricsClient}.
 */
@Category(XSlowTests.class)
public class MetricsClientTestRun extends ClientTestBase {

  private MetricsClient metricsClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    metricsClient = new MetricsClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    JsonObject metric = metricsClient.getMetric("user", "/apps/FakeApp/flows", "process.events", "aggregate=true");
    Assert.assertEquals(0, metric.get("data").getAsInt());
  }
}
