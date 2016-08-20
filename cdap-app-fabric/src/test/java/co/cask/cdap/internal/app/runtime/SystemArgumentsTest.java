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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.api.Resources;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SystemArguments}.
 */
public class SystemArgumentsTest {

  @Test
  public void testSystemResources() {
    Resources defaultResources = new Resources();

    // Nothing specified
    Resources resources = SystemArguments.getResources(ImmutableMap.<String, String>of(), defaultResources);
    Assert.assertEquals(defaultResources, resources);

    // Specify memory
    resources = SystemArguments.getResources(ImmutableMap.of("system.resources.memory", "10"), defaultResources);
    Assert.assertEquals(new Resources(10), resources);

    // Specify cores
    resources = SystemArguments.getResources(ImmutableMap.of("system.resources.cores", "8"), defaultResources);
    Assert.assertEquals(new Resources(defaultResources.getMemoryMB(), 8), resources);

    // Specify both memory and cores
    resources = SystemArguments.getResources(ImmutableMap.of("system.resources.memory", "10",
                                                             "system.resources.cores", "8"), defaultResources);
    Assert.assertEquals(new Resources(10, 8), resources);

    // Specify invalid memory
    resources = SystemArguments.getResources(ImmutableMap.of("system.resources.memory", "-10"), defaultResources);
    Assert.assertEquals(defaultResources, resources);

    // Specify invalid cores
    resources = SystemArguments.getResources(ImmutableMap.of("system.resources.cores", "abc"), defaultResources);
    Assert.assertEquals(defaultResources, resources);

    // Specify invalid memory and value cores
    resources = SystemArguments.getResources(ImmutableMap.of("system.resources.memory", "xyz",
                                                             "system.resources.cores", "8"), defaultResources);
    Assert.assertEquals(new Resources(defaultResources.getMemoryMB(), 8), resources);

    // Specify valid memory and invalid cores
    resources = SystemArguments.getResources(ImmutableMap.of("system.resources.memory", "10",
                                                             "system.resources.cores", "-8"), defaultResources);
    Assert.assertEquals(new Resources(10, defaultResources.getVirtualCores()), resources);

    // Specify invalid memory and invalid cores
    resources = SystemArguments.getResources(ImmutableMap.of("system.resources.memory", "-1",
                                                             "system.resources.cores", "-8"), defaultResources);
    Assert.assertEquals(defaultResources, resources);

  }
}
