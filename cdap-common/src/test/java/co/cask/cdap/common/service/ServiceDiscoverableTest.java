/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.service;

import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ServiceDiscoverable}
 */
public class ServiceDiscoverableTest {

  @Test
  public void testProgramId() throws Exception {
    ProgramId serviceId = new ApplicationId("ns", "app").service("s1");
    String discoverableName = ServiceDiscoverable.getName(serviceId);
    Assert.assertEquals("service.ns.app.s1", discoverableName);
    Assert.assertTrue(ServiceDiscoverable.isUserService(discoverableName));
    Assert.assertFalse(ServiceDiscoverable.isUserService("service1."));
    Assert.assertEquals(serviceId, ServiceDiscoverable.getId(discoverableName));
  }
}
