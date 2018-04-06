/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.guice.AppFabricTestModule;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Test for Provisioning Service.
 */
public class ProvisioningServiceTest {
  private static ProvisioningService provisioningService;

  @BeforeClass
  public static void setupClass() {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new AppFabricTestModule(cConf));
    provisioningService = injector.getInstance(ProvisioningService.class);
  }

  @Test
  public void testGetSpecs() {
    Collection<ProvisionerDetail> specs = provisioningService.getProvisionerDetails();
    Assert.assertEquals(1, specs.size());

    ProvisionerSpecification spec = new MockProvisioner().getSpec();
    ProvisionerDetail expected = new ProvisionerDetail(spec.getName(), spec.getDescription(), new ArrayList<>());
    Assert.assertEquals(expected, specs.iterator().next());

    Assert.assertEquals(expected, provisioningService.getProvisionerDetail("yarn"));
    Assert.assertNull(provisioningService.getProvisionerDetail("abc"));
  }
}
