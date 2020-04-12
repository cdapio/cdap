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
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteMonitorType;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.http.NettyHttpService;
import org.apache.tephra.TxConstants;
import org.apache.twill.api.Configs;
import org.apache.twill.api.TwillPreparer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Utility class to help extract system properties from the program runtime arguments.
 */
public final class SystemArguments {

  private static final Logger LOG = LoggerFactory.getLogger(SystemArguments.class);

  // Keys for container resources
  public static final String MEMORY_KEY = "system.resources.memory";
  public static final String CORES_KEY = "system.resources.cores";
  public static final String RESERVED_MEMORY_KEY_OVERRIDE = "system.resources.reserved.memory.override";

  // Keys for log levels
  private static final String LOG_LEVEL = "system.log.level";
  private static final String LOGGER_LOG_LEVEL_PREFIX = LOG_LEVEL + ".";

  // Keys for retry policy
  private static final String RETRY_POLICY_TYPE = "system." + Constants.Retry.TYPE;
  private static final String RETRY_POLICY_MAX_TIME_SECS = "system." + Constants.Retry.MAX_TIME_SECS;
  private static final String RETRY_POLICY_MAX_RETRIES = "system." + Constants.Retry.MAX_RETRIES;
  private static final String RETRY_POLICY_DELAY_BASE_MS = "system." + Constants.Retry.DELAY_BASE_MS;
  private static final String RETRY_POLICY_DELAY_MAX_MS = "system." + Constants.Retry.DELAY_MAX_MS;

  // Keys for transaction
  public static final String TRANSACTION_TIMEOUT = "system.data.tx.timeout";

  // Keys for metrics system
  public static final String METRICS_ENABLED = "system.metrics.enabled";
  public static final String METRICS_CONTEXT_TASK_INCLUDED = "system.metrics.context.task.included";
  private static final String YARN_MAX_APP_ATTEMPTS = "system.yarn.max.app.attempts";
  private static final String YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL
    = "system.yarn.attempt.failures.validity.interval";

  // Keys for http service
  public static final String SERVICE_THREADS = "system.service.threads";
  public static final String SERVICE_THREAD_KEEPALIVE_SECS = "system.service.thread.keepalive.secs";

  // Keys for profiles
  public static final String PROFILE_NAME = "system.profile.name";
  public static final String PROFILE_PROVISIONER = "system.profile.provisioner";
  public static final String PROFILE_PROPERTIES_PREFIX = "system.profile.properties.";

  /**
   * Runtime arguments for program impersonation
   */
  public static final String RUNTIME_KEYTAB_PATH = "system.runtime.keytab.path";
  public static final String RUNTIME_PRINCIPAL_NAME = "system.runtime.principal.name";

  // Runtime arguments for remote exeuction monitor type. Can be "ssh" or "url".
  public static final String RUNTIME_MONITOR_TYPE = "system.runtime.monitor.type";

  /**
   * Extracts log level settings from the given arguments. It extracts arguments prefixed with key
   * {@link #LOG_LEVEL} + {@code .}, with the remaining part of the key as the logger name, with the argument value
   * as the log level. Also, the key {@link #LOG_LEVEL} will be used to setup the log level of the root logger.
   */
  public static Map<String, Level> getLogLevels(Map<String, String> args) {
    Map<String, Level> logLevels = new HashMap<>();
    for (Map.Entry<String, String> entry : args.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(LOGGER_LOG_LEVEL_PREFIX)) {
        logLevels.put(key.substring(LOGGER_LOG_LEVEL_PREFIX.length()), Level.toLevel(entry.getValue()));
      }
    }
    String logLevel = args.get(LOG_LEVEL);
    if (logLevel != null) {
      logLevels.put(Logger.ROOT_LOGGER_NAME, Level.toLevel(logLevel));
    }
    return logLevels;
  }

  /**
   * If args contains {@link SystemArguments#METRICS_ENABLED}
   * we evaluate and return the boolean value, if the key is not preset we return the defaultValue
   * @param args runtime arguments
   * @param defaultValue value to return when key is not preset in the args
   * @return true if metrics is enabled
   */
  public static boolean isProgramMetricsEnabled(Map<String, String> args, boolean defaultValue) {
    String metricsEnabled = args.get(METRICS_ENABLED);
    return metricsEnabled == null ? defaultValue : Boolean.valueOf(metricsEnabled);
  }

  /**
   * Set the log level for the {@link LogAppenderInitializer}.
   *
   * @param args the arguments to use for looking up resources configurations
   * @param initializer the LogAppenderInitializer which will be used to set up the log level
   */
  public static void setLogLevel(Arguments args, LogAppenderInitializer initializer) {
    initializer.setLogLevels(getLogLevels(args.asMap()));
  }

  /**
   * Set the transaction timeout in the given arguments.
   */
  public static void setTransactionTimeout(Map<String, String> args, int timeout) {
    args.put(TRANSACTION_TIMEOUT, String.valueOf(timeout));
  }

  /**
   * Returns the transaction timeout based on the given arguments or, as fallback, the CConfiguration.
   *
   * @return the integer value of the argument system.data.tx.timeout, or if that is not given in the arguments,
   *         the value for data.tx.timeout from the CConfiguration.
   * @throws IllegalArgumentException if the transaction timeout exceeds the transaction timeout limit given by
   *         the {@link TxConstants.Manager#CFG_TX_MAX_TIMEOUT} setting in the {@link CConfiguration}
   */
  public static int getTransactionTimeout(Map<String, String> args, CConfiguration cConf) {
    Integer timeout = getPositiveInt(args, TRANSACTION_TIMEOUT, "transaction timeout");
    if (timeout == null) {
      return cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT);
    }
    int maxTimeout = cConf.getInt(TxConstants.Manager.CFG_TX_MAX_TIMEOUT);
    if (timeout > maxTimeout) {
      throw new IllegalArgumentException(String.format(
        "Transaction timeout (%s) of %d seconds must not exceed the transaction timeout limit (%s) of %d",
        TRANSACTION_TIMEOUT, timeout, TxConstants.Manager.CFG_TX_MAX_TIMEOUT, maxTimeout));
    }
    return timeout;
  }

  /**
   * Validates the custom transaction timeout, if specified in the given arguments.
   *
   * @throws IllegalArgumentException if the transaction timeout exceeds the transaction timeout limit given in the
   *         CConfiguration.
   */
  public static void validateTransactionTimeout(Map<String, String> args, CConfiguration cConf) {
    validateTransactionTimeout(args, cConf, null, null);
  }

  /**
   * Validates the custom transaction timeout, if specified in the given arguments. If scope and name are not null,
   * validates only for that given scope (for example, flowlet.myFlowlet).
   *
   * @throws IllegalArgumentException if the transaction timeout exceeds the transaction timeout limit given in the
   *         CConfiguration.
   */
  public static void validateTransactionTimeout(Map<String, String> args, CConfiguration cConf,
                                                @Nullable String scope, @Nullable String name) {
    String argName = TRANSACTION_TIMEOUT;
    if (scope != null && name != null) {
      argName = RuntimeArguments.addScope(scope, name, TRANSACTION_TIMEOUT);
    }
    Integer timeout = getPositiveInt(args, argName, "transaction timeout");
    if (timeout != null) {
      int maxTimeout = cConf.getInt(TxConstants.Manager.CFG_TX_MAX_TIMEOUT);
      if (timeout > maxTimeout) {
        throw new IllegalArgumentException(String.format(
          "Transaction timeout (%s) of %d seconds must not exceed the transaction timeout limit (%s) of %d",
          argName, timeout, TxConstants.Manager.CFG_TX_MAX_TIMEOUT, maxTimeout));
      }
    }
  }

  /**
   * Get the retry strategy for a program given its arguments and the CDAP defaults for the program type.
   *
   * @return the retry strategy to use for internal calls
   * @throws IllegalArgumentException if there is an invalid value for an argument
   */
  public static RetryStrategy getRetryStrategy(Map<String, String> args, ProgramType programType,
                                               CConfiguration cConf) {
    String keyPrefix;
    switch (programType) {
      case MAPREDUCE:
        keyPrefix = Constants.Retry.MAPREDUCE_PREFIX;
        break;
      case SPARK:
        keyPrefix = Constants.Retry.SPARK_PREFIX;
        break;
      case WORKFLOW:
        keyPrefix = Constants.Retry.WORKFLOW_PREFIX;
        break;
      case WORKER:
        keyPrefix = Constants.Retry.WORKER_PREFIX;
        break;
      case SERVICE:
        keyPrefix = Constants.Retry.SERVICE_PREFIX;
        break;
      case CUSTOM_ACTION:
        keyPrefix = Constants.Retry.CUSTOM_ACTION_PREFIX;
        break;
      default:
        throw new IllegalArgumentException("Invalid program type " + programType);
    }

    // Override from runtime arguments
    CConfiguration policyConf = CConfiguration.copy(cConf);
    String typeStr = args.get(RETRY_POLICY_TYPE);
    if (typeStr != null) {
      policyConf.set(keyPrefix + Constants.Retry.TYPE, typeStr);
    }
    int maxRetries = getNonNegativeInt(args, RETRY_POLICY_MAX_RETRIES, RETRY_POLICY_MAX_RETRIES, -1);
    if (maxRetries >= 0) {
      policyConf.setInt(keyPrefix + Constants.Retry.MAX_RETRIES, maxRetries);
    }
    long maxTimeSecs = getNonNegativeLong(args, RETRY_POLICY_MAX_TIME_SECS, RETRY_POLICY_MAX_TIME_SECS, -1L);
    if (maxTimeSecs >= 0) {
      policyConf.setLong(keyPrefix + Constants.Retry.MAX_TIME_SECS, maxTimeSecs);
    }
    long baseDelay = getNonNegativeLong(args, RETRY_POLICY_DELAY_BASE_MS, RETRY_POLICY_DELAY_BASE_MS, -1L);
    if (baseDelay >= 0) {
      policyConf.setLong(keyPrefix + Constants.Retry.DELAY_BASE_MS, baseDelay);
    }
    long maxDelay = getNonNegativeLong(args, RETRY_POLICY_DELAY_MAX_MS, RETRY_POLICY_DELAY_MAX_MS, -1L);
    if (maxDelay >= 0) {
      policyConf.setLong(keyPrefix + Constants.Retry.DELAY_MAX_MS, maxDelay);
    }

    return RetryStrategies.fromConfiguration(policyConf, keyPrefix);
  }

  /**
   * Returns the {@link Resources} based on configurations in the given arguments.
   *
   * Same as calling {@link #getResources(Map, Resources)} with first argument from {@link Arguments#asMap()}.
   */
  public static Resources getResources(Arguments args, @Nullable Resources defaultResources) {
    return getResources(args.asMap(), defaultResources);
  }

  /**
   * Returns the {@link Resources} based on configurations in the given arguments.
   *
   * @param args the arguments to use for looking up resources configurations
   * @param defaultResources default resources to use if resources configurations are missing from the arguments.
   *                         If it is {@code null}, the default values in {@link Resources} will be used.
   */
  public static Resources getResources(Map<String, String> args, @Nullable Resources defaultResources) {
    Integer memory = getPositiveInt(args, MEMORY_KEY, "memory size");
    Integer cores = getPositiveInt(args, CORES_KEY, "number of cores");
    defaultResources = defaultResources == null ? new Resources() : defaultResources;

    if (memory == null && cores == null) {
      return defaultResources;
    }
    return new Resources(memory != null ? memory : defaultResources.getMemoryMB(),
                         cores != null ? cores : defaultResources.getVirtualCores());
  }

  /**
   * Returns the twill configurations to be used in {@link TwillPreparer#withConfiguration(Map)} based on the
   * runtime arguments.
   */
  public static Map<String, String> getTwillApplicationConfigs(Map<String, String> args) {
    Map<String, String> result = new HashMap<>();

    Integer maxAttempts = getPositiveInt(args, YARN_MAX_APP_ATTEMPTS, YARN_MAX_APP_ATTEMPTS);
    if (maxAttempts != null) {
      result.put(Configs.Keys.YARN_MAX_APP_ATTEMPTS, maxAttempts.toString());
    }

    Long failureValidityInterval = getLong(args, YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL,
                                           YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL);
    if (failureValidityInterval != null) {
      result.put(Configs.Keys.YARN_ATTEMPT_FAILURES_VALIDITY_INTERVAL, failureValidityInterval.toString());
    }

    return result;
  }

  /**
   * Extracts and returns a configuration map containing container related Twill settings.
   *
   * @param args the arguments to use for looking up resources configurations
   * @param containerMemory the container memory size in MB
   * @return a map of configurations
   */
  public static Map<String, String> getTwillContainerConfigs(Map<String, String> args, int containerMemory) {
    Integer reservedMemory = getPositiveInt(args, RESERVED_MEMORY_KEY_OVERRIDE, "reserved memory size");
    if (reservedMemory == null) {
      return Collections.emptyMap();
    }
    int heapMemory = containerMemory - reservedMemory;
    if (heapMemory <= 0) {
      LOG.warn("Ignoring invalid reserved memory size '{}' from runtime arguments. " +
                 "It must be smaller than container memory size '{}'", reservedMemory, containerMemory);
      return Collections.emptyMap();
    }
    double ratio = ((double) heapMemory) / containerMemory;

    Map<String, String> config = new HashMap<>();
    config.put(Configs.Keys.JAVA_RESERVED_MEMORY_MB, reservedMemory.toString());
    config.put(Configs.Keys.HEAP_RESERVED_MIN_RATIO, String.format("%.2f", ratio));
    return config;
  }

  /**
   * Configures a {@link NettyHttpService.Builder} based on the given arguments.
   *
   * @param args the arguments to use for looking up http service configurations
   * @param builder a {@link NettyHttpService.Builder} to configure
   * @param <T> type of the {@link NettyHttpService.Builder}
   * @return the builder instance as provided through the {@code builder} argument
   */
  public static <T extends NettyHttpService.Builder> T configureNettyHttpService(Map<String, String> args, T builder) {
    Integer threads = getInt(args, SERVICE_THREADS, "http service executor thread pool size");
    if (threads != null) {
      builder.setExecThreadPoolSize(threads);
    }
    Long keepAliveSecs = getLong(args, SERVICE_THREAD_KEEPALIVE_SECS, "http service executor keep alive seconds");
    if (keepAliveSecs != null) {
      builder.setExecThreadKeepAliveSeconds(keepAliveSecs);
    }
    return builder;
  }

  /**
   * Get the profile id for the given program, given arguments for a run. All non-workflow program types will use the
   * native profile. Workflow program types will use whatever profile is specified in its arguments, or the native
   * profile if none is specified.
   *
   * @param programId program to get the profile for
   * @param args arguments for a program run
   * @return the profile id for the program run
   */
  public static ProfileId getProfileIdForProgram(ProgramId programId, Map<String, String> args) {
    if (isProfileAllowed(programId.getType())) {
      return getProfileIdFromArgs(programId.getNamespaceId(), args).orElse(ProfileId.NATIVE);
    }
    return ProfileId.NATIVE;
  }

  /**
   * Determine if compute profile is allowed to be applied on the given program type.
   *
   * @param programType programType to check compatibility
   * @return true if the program type can use profiles, otherwise false
   */
  public static boolean isProfileAllowed(ProgramType programType) {
    return programType == ProgramType.WORKFLOW
      || programType == ProgramType.MAPREDUCE
      || programType == ProgramType.SPARK
      || programType == ProgramType.WORKER;
  }

  /**
   * Get the profile id from namespace and the argument. Note that the profile name in the args is initially a scoped
   * name. If it is in system scope, this method will return a profile id which is in SYSTEM namespace, otherwise, it
   * will be in the given namespace.
   *
   * @param namespaceId namespace this profile is in
   * @param args arguments to look up
   * @return the profile id if it is present
   */
  public static Optional<ProfileId> getProfileIdFromArgs(NamespaceId namespaceId, Map<String, String> args) {
    if (args.containsKey(PROFILE_NAME)) {
      String scopedProfile = args.get(SystemArguments.PROFILE_NAME);
      ProfileId profileId = ProfileId.fromScopedName(namespaceId, scopedProfile);
      return Optional.of(profileId);
    }
    return Optional.empty();
  }

  /**
   * Adds any profile related arguments to the map
   *
   * @param args arguments to add to
   * @param profile the profile to add
   */
  public static void addProfileArgs(Map<String, String> args, Profile profile) {
    args.put(PROFILE_NAME, profile.getScopedName());
    args.put(PROFILE_PROVISIONER, profile.getProvisioner().getName());
    for (ProvisionerPropertyValue property : profile.getProvisioner().getProperties()) {
      String key = PROFILE_PROPERTIES_PREFIX + property.getName();
      if (!property.isEditable()) {
        args.put(key, property.getValue());
      } else if (!args.containsKey(key)) {
        args.put(key, property.getValue());
      }
    }
  }

  /**
   * Get the name of the provisioner for the profile
   *
   * @param args arguments
   * @return name of the provisioner for the profile
   */
  public static String getProfileProvisioner(Map<String, String> args) {
    return args.get(PROFILE_PROVISIONER);
  }

  /**
   * Gets the {@link RemoteMonitorType} from the given arguments.
   */
  public static RemoteMonitorType getRuntimeMonitorType(CConfiguration cConf, ProgramOptions programOptions) {
    if (cConf.get(Constants.RuntimeMonitor.MONITOR_URL) == null) {
      return RemoteMonitorType.SSH;
    }

    String type = programOptions.getUserArguments().getOption(RUNTIME_MONITOR_TYPE);
    if (type == null) {
      // If not in the arguments, read it from cConf based on the provisioner name
      String provisioner = getProfileProvisioner(programOptions.getArguments().asMap());
      type = cConf.get(String.format("%s%s", Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX, provisioner));
    }
    if (type == null) {
      return RemoteMonitorType.SSH;
    }

    try {
      return RemoteMonitorType.valueOf(type.toUpperCase());
    } catch (Exception e) {
      LOG.warn("Unsupported runtime monitor type {}. Default to SSH", type, e);
      return RemoteMonitorType.SSH;
    }
  }

  /**
   * Get the properties for the profile
   *
   * @param args arguments
   * @return properties for the profile
   */
  public static Map<String, String> getProfileProperties(Map<String, String> args) {
    Map<String, String> properties = new HashMap<>();
    for (Map.Entry<String, String> arg : args.entrySet()) {
      if (arg.getKey().startsWith(PROFILE_PROPERTIES_PREFIX)) {
        String key = arg.getKey().substring(PROFILE_PROPERTIES_PREFIX.length());
        properties.put(key, arg.getValue());
      }
    }
    return properties;
  }

  /**
   * Gets a positive integer value from the given map using the given key.
   * If there is no such key or if the value is not positive, returns {@code null}.
   */
  private static Integer getPositiveInt(Map<String, String> map, String key, String description) {
    Integer val = getInt(map, key, description);
    if (val != null && val <= 0) {
      LOG.warn("Ignoring invalid {} '{}' from runtime arguments. It must be a positive integer.",
               description, val);
      return null;
    }
    return val;
  }

  /**
   * Gets a non-negative (can be 0) integer value from the given map using the given key.
   * If there is no such key or if the value is negative, returns the default.
   */
  private static int getNonNegativeInt(Map<String, String> map, String key, String description, int defaultVal) {
    Integer val = getInt(map, key, description);
    if (val == null) {
      return defaultVal;
    } else if (val < 0) {
      LOG.warn("Ignoring invalid {} '{}' from runtime arguments. It must be a non-negative integer.",
               description, val);
      return defaultVal;
    }
    return val;
  }

  /**
   * Gets a non-negative (can be 0) long value from the given map using the given key.
   * If there is no such key or if the value is negative, returns the default.
   */
  private static long getNonNegativeLong(Map<String, String> map, String key, String description, long defaultVal) {
    Long val = getLong(map, key, description);
    if (val == null) {
      return defaultVal;
    } else if (val < 0) {
      LOG.warn("Ignoring invalid {} '{}' from runtime arguments. It must be a non-negative long.",
               description, val);
      return defaultVal;
    }
    return val;
  }

  @Nullable
  private static Integer getInt(Map<String, String> map, String key, String description) {
    String value = map.get(key);
    if (value == null) {
      return null;
    }

    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      // Only the log the stack trace as debug, as usually it's not needed.
      LOG.warn("Ignoring invalid {} '{}' from runtime arguments. It could not be parsed as an integer.",
               description, value);
      LOG.debug("Invalid {}", description, e);
    }

    return null;
  }

  @Nullable
  private static Long getLong(Map<String, String> map, String key, String description) {
    String value = map.get(key);
    if (value == null) {
      return null;
    }

    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      // Only the log the stack trace as debug, as usually it's not needed.
      LOG.warn("Ignoring invalid {} '{}' from runtime arguments. It could not be parsed as a long.",
               description, value);
      LOG.debug("Invalid {}", description, e);
    }

    return null;
  }

  private SystemArguments() {
  }
}
