/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import io.kubernetes.client.custom.Quantity;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link KubeTwillPreparer}. The test is disabled by default since it requires a running
 * kubernetes cluster for the test to run. This class is kept for development purpose.
 */
public class KubeTwillPreparerTest {

  @Test
  public void testVirtualCoresToCpu() {
    int vCoresMilli = 200;
    Quantity cpuQuantity = KubeTwillPreparer.vCoresToCpuQuantity(vCoresMilli);
    Quantity expectedCpuQuantity = new Quantity(String.valueOf((double) vCoresMilli / 1000.0));
    Assert.assertTrue(cpuQuantity.equals(expectedCpuQuantity));
  }
}
