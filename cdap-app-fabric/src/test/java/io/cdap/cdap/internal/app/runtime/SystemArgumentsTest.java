/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import ch.qos.logback.classic.Level;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.common.service.RetryStrategyType;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.twill.api.Configs;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * Unit tests for {@link SystemArguments}.
 */
public class SystemArgumentsTest {

  @Test
  public void testGetProgramProfile() {
    ProfileId profileId = NamespaceId.DEFAULT.profile("p");
    Map<String, String> args = Collections.singletonMap(SystemArguments.PROFILE_NAME, profileId.getScopedName());

    ApplicationId appId = NamespaceId.DEFAULT.app("a");
    ProgramId mrId = appId.mr("mr");
    ProgramId serviceId = appId.service("serv");
    ProgramId sparkId = appId.spark("spark");
    ProgramId workerId = appId.worker("worker");
    ProgramId workflowID = appId.workflow("wf");

    Assert.assertEquals(profileId, SystemArguments.getProfileIdForProgram(mrId, args));
    Assert.assertEquals(ProfileId.NATIVE, SystemArguments.getProfileIdForProgram(serviceId, args));
    Assert.assertEquals(profileId, SystemArguments.getProfileIdForProgram(sparkId, args));
    Assert.assertEquals(profileId, SystemArguments.getProfileIdForProgram(workerId, args));
    Assert.assertEquals(profileId, SystemArguments.getProfileIdForProgram(workflowID, args));
  }

  @Test
  public void testSystemResources() {
    Resources defaultResources = new Resources();

    // Nothing specified
    Resources resources = SystemArguments.getResources(ImmutableMap.of(), defaultResources);
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
  public void testGetTwillApplicationConfigs() {
    // disable cleanup config specified
    Map<String, String> configs = SystemArguments.getTwillApplicationConfigs(
      ImmutableMap.of(SystemArguments.RUNTIME_CLEANUP_DISABLED, "true"));
    Assert.assertTrue("unexpected value for config: " + SystemArguments.RUNTIME_CLEANUP_DISABLED,
                      Boolean.parseBoolean(configs.get(SystemArguments.RUNTIME_CLEANUP_DISABLED)));

    // disable cleanup config not specified
    configs = SystemArguments.getTwillApplicationConfigs(ImmutableMap.of(SystemArguments.MEMORY_KEY, "10"));
    Assert.assertTrue(configs.isEmpty());
  }

  @Test
  public void testRetryStrategies() {
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
    Assert.assertTrue(SystemArguments.getLogLevels(Collections.emptyMap()).isEmpty());

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
    ProfileId expected = NamespaceId.DEFAULT.profile("MyProfile");
    args.put("system.profile.name", expected.getProfile());
    Assert.assertEquals(expected, SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT, args).get());

    // put a profile with scope SYSTEM, the profile we get should be in system namespace
    expected = NamespaceId.SYSTEM.profile("MyProfile");
    args.put("system.profile.name", expected.getScopedName());
    Assert.assertEquals(expected, SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT, args).get());

    // put a profile with scope USER, the profile we get should be in the user namespace
    expected = NamespaceId.DEFAULT.profile("MyProfile");
    args.put("system.profile.name", expected.getScopedName());
    Assert.assertEquals(expected, SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT, args).get());
  }

  @Test
  public void testResolveScopedUserArguments() {
    ProgramId workflowId = NamespaceId.DEFAULT.app("myApp").workflow("myWf");
    Map<String, String> systemArgs = Collections.singletonMap(Constants.CLUSTER_NAME, "c1");
    Map<String, String> userArgs = ImmutableMap.<String, String>builder()
        .put("k", "unscoped")
        .put("cluster.c1.k", "fromCluster")
        .put("cluster.other.k", "otherCluster")
        .put("app.myApp.k", "fromApp")
        .put("app.otherApp.k", "otherApp")
        .put("workflow.myWf.k", "fromWorkflow")
        .put("workflow.otherWf.k", "otherWorkflow")
        .put("clusterVsApp", "unscoped")
        .put("cluster.c1.clusterVsApp", "fromCluster")
        .put("app.myApp.clusterVsApp", "fromApp")
        .put("cluster.c1.app.myApp.workflow.myWf.chained", "chainedVal")
        .put("clusterOnly", "baseCluster")
        .put("cluster.c1.clusterOnly", "clusterOverride")
        .put("appOnly", "baseApp")
        .put("app.myApp.appOnly", "appOverride")
        .put("wildWf", "baseWild")
        .put("workflow.*.wildWf", "wildWfOverride")
        .build();

    Map<String, String> resolved =
        SystemArguments.resolveScopedUserArguments(workflowId, systemArgs, userArgs);
    Assert.assertEquals("fromWorkflow", resolved.get("k"));
    Assert.assertEquals("fromApp", resolved.get("clusterVsApp"));
    Assert.assertEquals("chainedVal", resolved.get("chained"));
    Assert.assertEquals("clusterOverride", resolved.get("clusterOnly"));
    Assert.assertEquals("appOverride", resolved.get("appOnly"));
    Assert.assertEquals("wildWfOverride", resolved.get("wildWf"));

    // Empty, absent, or null systemArgs skips cluster scope resolution without throwing
    for (Map<String, String> noClusterSysArgs : Arrays.<Map<String, String>>asList(
        Collections.singletonMap(Constants.CLUSTER_NAME, ""),
        Collections.emptyMap(),
        null)) {
      Map<String, String> noClusterResolved =
          SystemArguments.resolveScopedUserArguments(workflowId, noClusterSysArgs, userArgs);
      Assert.assertEquals("baseCluster", noClusterResolved.get("clusterOnly"));
      Assert.assertEquals("appOverride", noClusterResolved.get("appOnly"));
      Assert.assertEquals("fromWorkflow", noClusterResolved.get("k"));
    }

    // Non-workflow program type resolves its own scope prefix (spark.<name>.)
    ProgramId sparkId = NamespaceId.DEFAULT.app("myApp").spark("mySpark");
    Map<String, String> sparkResolved = SystemArguments.resolveScopedUserArguments(
        sparkId, Collections.emptyMap(),
        ImmutableMap.of("p", "base", "spark.mySpark.p", "fromSpark", "workflow.mySpark.p", "wrong"));
    Assert.assertEquals("fromSpark", sparkResolved.get("p"));
  }

  @Test
  public void testUsesRuntimeToken() {
    ProgramId workflowId = NamespaceId.DEFAULT.app("myApp").workflow("myWf");
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    cConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "dataproc",
        RuntimeMonitorType.URL.name());

    Map<String, String> scopedSysArgs = ImmutableMap.of(
        SystemArguments.PROFILE_PROVISIONER, "dataproc",
        ProgramOptionConstants.RUNTIME_SCOPES, "{}");
    ProgramOptions urlOptions = new SimpleProgramOptions(
        workflowId, new BasicArguments(scopedSysArgs), new BasicArguments());

    // Internal auth on + scopes present + URL monitor type -> true
    Assert.assertTrue(SystemArguments.usesRuntimeToken(cConf, urlOptions));

    // Internal auth off -> false
    CConfiguration noInternalAuth = CConfiguration.copy(cConf);
    noInternalAuth.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    Assert.assertFalse(SystemArguments.usesRuntimeToken(noInternalAuth, urlOptions));

    // Missing scopes -> false
    ProgramOptions noScopes = new SimpleProgramOptions(
        workflowId,
        new BasicArguments(Collections.singletonMap(
            SystemArguments.PROFILE_PROVISIONER, "dataproc")),
        new BasicArguments());
    Assert.assertFalse(SystemArguments.usesRuntimeToken(cConf, noScopes));

    // RUNTIME_SCOPES in user args (not system args) -> false
    ProgramOptions userArgScopes = new SimpleProgramOptions(
        workflowId,
        new BasicArguments(Collections.singletonMap(
            SystemArguments.PROFILE_PROVISIONER, "dataproc")),
        new BasicArguments(Collections.singletonMap(
            ProgramOptionConstants.RUNTIME_SCOPES, "{}")));
    Assert.assertFalse(SystemArguments.usesRuntimeToken(cConf, userArgScopes));

    // User arg system.runtime.monitor.type=SSH cannot override operator-configured URL
    ProgramOptions userArgSshOnUrlConf = new SimpleProgramOptions(
        workflowId,
        new BasicArguments(scopedSysArgs),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.SSH.name())));
    Assert.assertEquals(RuntimeMonitorType.URL,
        SystemArguments.getRuntimeMonitorType(cConf, userArgSshOnUrlConf));
    Assert.assertTrue(SystemArguments.usesRuntimeToken(cConf, userArgSshOnUrlConf));

    // Operator-configured SSH cannot be overridden by user arg system.runtime.monitor.type=URL
    CConfiguration sshOperatorConf = CConfiguration.copy(cConf);
    sshOperatorConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "dataproc",
        RuntimeMonitorType.SSH.name());
    ProgramOptions userArgUrlOnSshConf = new SimpleProgramOptions(
        workflowId,
        new BasicArguments(scopedSysArgs),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.URL.name())));
    Assert.assertEquals(RuntimeMonitorType.SSH,
        SystemArguments.getRuntimeMonitorType(sshOperatorConf, userArgUrlOnSshConf));
    Assert.assertFalse(SystemArguments.usesRuntimeToken(sshOperatorConf, userArgUrlOnSshConf));

    // Only when operator config omits monitor.type.<provisioner> does user arg select URL vs SSH
    CConfiguration unsetMonitorTypeConf = CConfiguration.copy(cConf);
    unsetMonitorTypeConf.unset(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "dataproc");
    Assert.assertEquals(RuntimeMonitorType.SSH,
        SystemArguments.getRuntimeMonitorType(unsetMonitorTypeConf, urlOptions));
    Assert.assertFalse(SystemArguments.usesRuntimeToken(unsetMonitorTypeConf, urlOptions));
    Assert.assertEquals(RuntimeMonitorType.URL,
        SystemArguments.getRuntimeMonitorType(unsetMonitorTypeConf, userArgUrlOnSshConf));
    Assert.assertTrue(SystemArguments.usesRuntimeToken(unsetMonitorTypeConf, userArgUrlOnSshConf));
    Assert.assertEquals(RuntimeMonitorType.SSH,
        SystemArguments.getRuntimeMonitorType(unsetMonitorTypeConf, userArgSshOnUrlConf));
    Assert.assertFalse(SystemArguments.usesRuntimeToken(unsetMonitorTypeConf, userArgSshOnUrlConf));

    // Missing or blank provisioner does not probe monitor.type.null and falls back to user arg
    CConfiguration nullKeyConf = CConfiguration.copy(unsetMonitorTypeConf);
    nullKeyConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "null",
        RuntimeMonitorType.URL.name());
    ProgramOptions missingProvisioner = new SimpleProgramOptions(
        workflowId, new BasicArguments(), new BasicArguments());
    Assert.assertEquals(RuntimeMonitorType.SSH,
        SystemArguments.getRuntimeMonitorType(nullKeyConf, missingProvisioner));
    ProgramOptions missingProvisionerUserUrl = new SimpleProgramOptions(
        workflowId, new BasicArguments(),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.URL.name())));
    Assert.assertEquals(RuntimeMonitorType.URL,
        SystemArguments.getRuntimeMonitorType(unsetMonitorTypeConf, missingProvisionerUserUrl));
    ProgramOptions blankProvisionerUserUrl = new SimpleProgramOptions(
        workflowId,
        new BasicArguments(Collections.singletonMap(SystemArguments.PROFILE_PROVISIONER, "")),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.URL.name())));
    Assert.assertEquals(RuntimeMonitorType.URL,
        SystemArguments.getRuntimeMonitorType(unsetMonitorTypeConf, blankProvisionerUserUrl));

    // Invalid operator value defaults to SSH and does not fall back to user arg URL
    CConfiguration invalidOperatorConf = CConfiguration.copy(cConf);
    invalidOperatorConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "dataproc", "BOGUS");
    Assert.assertEquals(RuntimeMonitorType.SSH,
        SystemArguments.getRuntimeMonitorType(invalidOperatorConf, userArgUrlOnSshConf));
    Assert.assertFalse(SystemArguments.usesRuntimeToken(invalidOperatorConf, userArgUrlOnSshConf));

    // Operator config with surrounding whitespace is trimmed cleanly
    CConfiguration paddedUrlConf = CConfiguration.copy(cConf);
    paddedUrlConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "dataproc", "  url  ");
    Assert.assertEquals(RuntimeMonitorType.URL,
        SystemArguments.getRuntimeMonitorType(paddedUrlConf, userArgSshOnUrlConf));

    // Scopes present, but MONITOR_URL unset (falls back to SSH) -> false
    CConfiguration noMonitorUrl = CConfiguration.copy(cConf);
    noMonitorUrl.unset(Constants.RuntimeMonitor.MONITOR_URL);
    Assert.assertFalse(SystemArguments.usesRuntimeToken(noMonitorUrl, urlOptions));
  }
}
