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

package io.cdap.cdap.master.environment.k8s;

import org.apache.twill.api.ResourceSpecification;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KubeMasterEnvironmentTest {
  @Test
  public void testGetDefaultResourceSpecification() {
    Map<String, String> conf = new HashMap<>();
    ResourceSpecification spec = null;

    // Empty conf, thus using default values
    spec = KubeMasterEnvironment.getDefaultResourceSpecification(conf);
    Assert.assertEquals(
      Integer.valueOf(KubeMasterEnvironment.DEFAULT_POD_REQUEST_CPU_MILLIS).intValue(),
      spec.getVirtualCores());
    Assert.assertEquals(
      Integer.valueOf(KubeMasterEnvironment.DEFAULT_POD_REQUEST_MEMORY_MB).intValue(),
      spec.getMemorySize());

    // Set the values in conf and verify they are used
    int cpuMillis = 500;
    int memoryMB = 128;
    conf.put(KubeMasterEnvironment.POD_REQUEST_CPU_MILLIS, Integer.toString(cpuMillis));
    conf.put(KubeMasterEnvironment.POD_REQUEST_MEMORY_MB, Integer.toString(memoryMB));
    spec = KubeMasterEnvironment.getDefaultResourceSpecification(conf);
    Assert.assertEquals(cpuMillis, spec.getVirtualCores());
    Assert.assertEquals(memoryMB, spec.getMemorySize());
  }
}
