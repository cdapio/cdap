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

import co.cask.cdap.client.MonitorClient;
import co.cask.cdap.proto.SystemServiceMeta;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

/**
 * Test for {@link MonitorClient}.
 */
@Category(XSlowTests.class)
public class MonitorClientTestRun extends ClientTestBase {

  private MonitorClient monitorClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    monitorClient = new MonitorClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    List<SystemServiceMeta> services = monitorClient.listSystemServices();
    Assert.assertTrue(services.size() > 0);

    String someService = services.get(0).getName();
    String serviceStatus = monitorClient.getSystemServiceStatus(someService);
    Assert.assertEquals("OK", serviceStatus);

    int systemServiceInstances = monitorClient.getSystemServiceInstances(someService);
    monitorClient.setSystemServiceInstances(someService, 1);
  }
}
