/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.Resources;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.twill.api.Configs;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests for various configurations that can affect MR task container resources setting.
 */
public class MapReduceResourcesTest {

  @Test
  public void testDefault() {
    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = new Configuration();

    // Nothing is set. It should be using the default.
    MapReduceRuntimeService.TaskType.MAP.configure(hConf, cConf, Collections.emptyMap(), null);
    int maxHeapSize = org.apache.twill.internal.utils.Resources.computeMaxHeapSize(
      Job.DEFAULT_MAP_MEMORY_MB, cConf.getInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB),
      cConf.getDouble(Configs.Keys.HEAP_RESERVED_MIN_RATIO));

    validateResources(cConf, hConf, Job.DEFAULT_MAP_MEMORY_MB, Job.DEFAULT_MAP_CPU_VCORES, maxHeapSize);

    // Sets the reserved memory via task arguments
    hConf = new Configuration();
    int reservedMemory = Job.DEFAULT_MAP_MEMORY_MB / 2;
    MapReduceRuntimeService.TaskType.MAP.configure(
      hConf, cConf, Collections.singletonMap("system.resources.reserved.memory.override",
                                             String.valueOf(reservedMemory)), null);

    validateResources(cConf, hConf, Job.DEFAULT_MAP_MEMORY_MB, Job.DEFAULT_MAP_CPU_VCORES,
                      Job.DEFAULT_MAP_MEMORY_MB - reservedMemory);
  }

  @Test
  public void testContextResources() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB, 300);
    Configuration hConf = new Configuration();

    // Resources is set through context object
    // At runtime time, it is either from spec, from runtime arg of the program
    // or from the context.setResources call in the initialize() method)
    Resources resources = new Resources(2345, 8);
    MapReduceRuntimeService.TaskType.MAP.configure(hConf, cConf, Collections.emptyMap(), resources);
    int maxHeapSize = org.apache.twill.internal.utils.Resources.computeMaxHeapSize(
      2345, cConf.getInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB), 0);

    validateResources(cConf, hConf, 2345, 8, maxHeapSize);

    // Set the reserved memory via task arguments
    hConf = new Configuration();
    MapReduceRuntimeService.TaskType.MAP.configure(
      hConf, cConf, Collections.singletonMap("system.resources.reserved.memory.override", "1000"), resources);

    validateResources(cConf, hConf, 2345, 8, 2345 - 1000);
  }

  @Test
  public void testProgrammatic() {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB, 300);
    Configuration hConf = new Configuration();

    hConf.setInt(Job.MAP_MEMORY_MB, 3000);
    hConf.setInt(Job.MAP_CPU_VCORES, 5);

    // Always use configurations setup programmatically via job conf.
    MapReduceRuntimeService.TaskType.MAP.configure(hConf, cConf, Collections.emptyMap(), null);
    int maxHeapSize = org.apache.twill.internal.utils.Resources.computeMaxHeapSize(
      3000, cConf.getInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB), 0);

    validateResources(cConf, hConf, 3000, 5, maxHeapSize);

    // Even resources is provided via context, it is ignored.
    hConf = new Configuration();
    hConf.setInt(Job.MAP_MEMORY_MB, 3000);
    hConf.setInt(Job.MAP_CPU_VCORES, 5);
    MapReduceRuntimeService.TaskType.MAP.configure(hConf, cConf, Collections.emptyMap(), new Resources(1234));
    maxHeapSize = org.apache.twill.internal.utils.Resources.computeMaxHeapSize(
      3000, cConf.getInt(Configs.Keys.JAVA_RESERVED_MEMORY_MB), 0);

    validateResources(cConf, hConf, 3000, 5, maxHeapSize);

    // Set the reserved memory via task arguments
    hConf = new Configuration();
    hConf.setInt(Job.MAP_MEMORY_MB, 3000);
    hConf.setInt(Job.MAP_CPU_VCORES, 5);
    MapReduceRuntimeService.TaskType.MAP.configure(
      hConf, cConf, Collections.singletonMap("system.resources.reserved.memory.override", "2000"), null);

    validateResources(cConf, hConf, 3000, 5, 3000 - 2000);
  }

  private void validateResources(CConfiguration cConf, Configuration hConf,
                                 int expectedMemoryMB, int expectedVcores, int expectedMaxHeap) {
    Assert.assertEquals(expectedMemoryMB, hConf.getInt(Job.MAP_MEMORY_MB, Job.DEFAULT_MAP_MEMORY_MB));
    Assert.assertEquals(expectedVcores, hConf.getInt(Job.MAP_CPU_VCORES, Job.DEFAULT_MAP_CPU_VCORES));

    String expectedJvmOpts = cConf.get(Constants.AppFabric.PROGRAM_JVM_OPTS) + " -Xmx" + expectedMaxHeap + "m";
    Assert.assertEquals(expectedJvmOpts, hConf.get(Job.MAP_JAVA_OPTS));
  }
}
