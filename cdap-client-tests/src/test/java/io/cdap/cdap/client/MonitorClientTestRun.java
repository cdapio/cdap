/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.proto.Containers;
import co.cask.cdap.proto.SystemServiceMeta;
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

    // check that all the system services can have their status individually retrieved
    for (SystemServiceMeta service : services) {
      Assert.assertEquals("OK", monitorClient.getSystemServiceStatus(service.getName()));
    }

    String someService = services.get(0).getName();
    List<Containers.ContainerInfo> containers = monitorClient.getSystemServiceLiveInfo(someService).getContainers();
    Assert.assertNotNull(containers);
    Assert.assertTrue(containers.isEmpty());
    Assert.assertEquals(0, monitorClient.getSystemServiceInstances(someService));
    monitorClient.setSystemServiceInstances(someService, 1);
    monitorClient.getSystemServiceInstances(someService);
    Assert.assertTrue(monitorClient.allSystemServicesOk());
  }
}
