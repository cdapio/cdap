/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.reactor.client;

import com.continuuity.client.MetricsClient;
import com.continuuity.client.MonitorClient;
import com.continuuity.client.config.ReactorClientConfig;
import com.continuuity.proto.SystemServiceMeta;
import com.continuuity.reactor.client.common.ClientTestBase;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Test for {@link MetricsClient}.
 */
public class MetricsClientTest extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsClientTest.class);

  private MetricsClient metricsClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();

    metricsClient = new MetricsClient(new ReactorClientConfig("localhost"));
  }

  @Test
  public void testAll() throws Exception {
    JsonObject metric = metricsClient.getMetric("user", "/apps/FakeApp/flows", "process.events", "aggregate=true");
    Assert.assertEquals(0, metric.get("data").getAsInt());
  }
}
