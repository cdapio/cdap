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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.common.service.RetryStrategyType;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

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

  @Test
  public void testRetryStrategies() throws InterruptedException {
    CConfiguration cConf = CConfiguration.create();
    Map<String, String> args = Collections.emptyMap();

    // Get default, expect exponential back-off behavior, until the max delay
    RetryStrategy strategy = SystemArguments.getRetryStrategy(args, ProgramType.CUSTOM_ACTION, cConf);
    long startTime = System.currentTimeMillis();
    Assert.assertEquals(1000L, strategy.nextRetry(1, startTime));
    Assert.assertEquals(2000L, strategy.nextRetry(2, startTime));
    Assert.assertEquals(4000L, strategy.nextRetry(3, startTime));
    Assert.assertEquals(8000L, strategy.nextRetry(4, startTime));
    Assert.assertEquals(16000L, strategy.nextRetry(5, startTime));
    Assert.assertEquals(30000L, strategy.nextRetry(6, startTime));
    Assert.assertEquals(30000L, strategy.nextRetry(7, startTime));
    // It should give up (returning -1) when exceeding the max retries
    Assert.assertEquals(-1L, strategy.nextRetry(1001, startTime));

    // Override the strategy type and max retry time
    args = ImmutableMap.of(
      "system." + Constants.Retry.TYPE, RetryStrategyType.FIXED_DELAY.toString(),
      "system." + Constants.Retry.MAX_TIME_SECS, "5"
    );
    strategy = SystemArguments.getRetryStrategy(args, ProgramType.CUSTOM_ACTION, cConf);
    startTime = System.currentTimeMillis();
    // Expects the delay doesn't change
    Assert.assertEquals(1000L, strategy.nextRetry(1, startTime));
    Assert.assertEquals(1000L, strategy.nextRetry(2, startTime));
    Assert.assertEquals(1000L, strategy.nextRetry(3, startTime));
    Assert.assertEquals(1000L, strategy.nextRetry(4, startTime));

    // Should give up (returning -1) after passing the max retry time
    Assert.assertEquals(-1L, strategy.nextRetry(1, startTime - 6000));
  }
}
