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

import ch.qos.logback.classic.Level;
import co.cask.cdap.api.Resources;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.common.service.RetryStrategyType;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.Configs;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
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
    resources = SystemArguments.getResources(ImmutableMap.of(SystemArguments.MEMORY_KEY, "10"), defaultResources);
    Assert.assertEquals(new Resources(10), resources);

    // Specify cores
    resources = SystemArguments.getResources(ImmutableMap.of(SystemArguments.CORES_KEY, "8"), defaultResources);
    Assert.assertEquals(new Resources(defaultResources.getMemoryMB(), 8), resources);

    // Specify both memory and cores
    resources = SystemArguments.getResources(ImmutableMap.of(SystemArguments.MEMORY_KEY, "10",
                                                             SystemArguments.CORES_KEY, "8"), defaultResources);
    Assert.assertEquals(new Resources(10, 8), resources);

    // Specify invalid memory
    resources = SystemArguments.getResources(ImmutableMap.of(SystemArguments.MEMORY_KEY, "-10"), defaultResources);
    Assert.assertEquals(defaultResources, resources);

    // Specify invalid cores
    resources = SystemArguments.getResources(ImmutableMap.of(SystemArguments.CORES_KEY, "abc"), defaultResources);
    Assert.assertEquals(defaultResources, resources);

    // Specify invalid memory and value cores
    resources = SystemArguments.getResources(ImmutableMap.of(SystemArguments.MEMORY_KEY, "xyz",
                                                             SystemArguments.CORES_KEY, "8"), defaultResources);
    Assert.assertEquals(new Resources(defaultResources.getMemoryMB(), 8), resources);

    // Specify valid memory and invalid cores
    resources = SystemArguments.getResources(ImmutableMap.of(SystemArguments.MEMORY_KEY, "10",
                                                             SystemArguments.CORES_KEY, "-8"), defaultResources);
    Assert.assertEquals(new Resources(10, defaultResources.getVirtualCores()), resources);

    // Specify invalid memory and invalid cores
    resources = SystemArguments.getResources(ImmutableMap.of(SystemArguments.MEMORY_KEY, "-1",
                                                             SystemArguments.CORES_KEY, "-8"), defaultResources);
    Assert.assertEquals(defaultResources, resources);

    // Specify reserved memory size
    Map<String, String> configs = SystemArguments.getTwillContainerConfigs(
      ImmutableMap.of(SystemArguments.RESERVED_MEMORY_KEY_OVERRIDE, "200"), 300);
    Assert.assertEquals(ImmutableMap.of(Configs.Keys.JAVA_RESERVED_MEMORY_MB, "200",
                                        Configs.Keys.HEAP_RESERVED_MIN_RATIO, "0.33"),
                        configs);

    // Specify invalid reserved memory size
    configs = SystemArguments.getTwillContainerConfigs(
      ImmutableMap.of(SystemArguments.RESERVED_MEMORY_KEY_OVERRIDE, "-1"), 300);
    Assert.assertTrue(configs.isEmpty());

    // Specify >= container memory size
    configs = SystemArguments.getTwillContainerConfigs(
      ImmutableMap.of(SystemArguments.RESERVED_MEMORY_KEY_OVERRIDE, "300"), 300);
    Assert.assertTrue(configs.isEmpty());
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

  @Test
  public void testLogLevels() {
    Assert.assertTrue(SystemArguments.getLogLevels(Collections.<String, String>emptyMap()).isEmpty());

    Map<String, String> args = ImmutableMap.of(
      "system.log.level", "DEBUG",
      "system.log.level.logger.info", "INFO",
      "system.log.level.logger.warn", "WARN",
      "system.log.leveldummyKey", "ERROR"     // <-- This should get picked
    );

    Map<String, Level> expected = ImmutableMap.of(
      Logger.ROOT_LOGGER_NAME, Level.DEBUG,
      "logger.info", Level.INFO,
      "logger.warn", Level.WARN);

    Assert.assertEquals(expected, SystemArguments.getLogLevels(args));
  }

  @Test
  public void testGetProfileId() {
    // should get null profile id if the args is empty
    Assert.assertFalse(SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT, Collections.emptyMap()).isPresent());
    Map<String, String> args = new HashMap<>();
    args.put("system.log.level", "DEBUG");
    args.put("system.log.leveldummyKey", "ERROR");

    // Having other unrelated args should also get null profile id
    Assert.assertFalse(SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT, args).isPresent());

    // without scope the profile will be considered in user scope
    args.put("system.profile.name", "MyProfile");
    ProfileId expected = NamespaceId.DEFAULT.profile("MyProfile");
    Assert.assertEquals(expected, SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT, args).get());

    // put a profile with scope SYSTEM, the profile we get should be in system namespace
    args.put("system.profile.name", "SYSTEM:MyProfile");
    expected = NamespaceId.SYSTEM.profile("MyProfile");
    Assert.assertEquals(expected, SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT, args).get());

    // put a profile with scope USER, the profile we get should be in the user namespace
    args.put("system.profile.name", "USER:MyProfile");
    expected = NamespaceId.DEFAULT.profile("MyProfile");
    Assert.assertEquals(expected, SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT, args).get());
  }
}
