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
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Utility class to help extract system properties from the program runtime arguments.
 */
public final class SystemArguments {

  private static final Logger LOG = LoggerFactory.getLogger(SystemArguments.class);

  private static final String MEMORY_KEY = "system.resources.memory";
  private static final String CORES_KEY = "system.resources.cores";
  private static final String LOG_LEVEL = "system.log.level";
  public static final String TRANSACTION_TIMEOUT = "system.data.tx.timeout";

  /**
   * Set the log level for the {@link LogAppenderInitializer}.
   *
   * Same as calling {@link #setLogLevel(Map, LogAppenderInitializer)} with first argument from
   * {@link Arguments#asMap()}
   */
  public static void setLogLevel(Arguments args, LogAppenderInitializer initializer) {
    setLogLevel(args.asMap(), initializer);
  }

  /**
   * Set the log level for the {@link LogAppenderInitializer}.
   * @param args the arguments to use for looking up resources configurations
   * @param initializer the LogAppenderInitializer which will be used to set up the log level
   */
  public static void setLogLevel(Map<String, String> args, LogAppenderInitializer initializer) {
    Map<String, String> logPairs = new HashMap<>();
    for (Map.Entry<String, String> entry : args.entrySet()) {
      String key = entry.getKey();
      if (!key.equals(LOG_LEVEL) && key.startsWith(LOG_LEVEL)) {
        logPairs.put(entry.getKey().substring(LOG_LEVEL.length() + 1), entry.getValue());
      }
    }
    String logLevel = args.get(LOG_LEVEL);
    if (logLevel != null) {
      logPairs.put(Logger.ROOT_LOGGER_NAME, logLevel);
    }
    initializer.setLogLevels(logPairs);
  }

  /**
   * Set the transaction timeout in the given arguments.
   */
  public static void setTransactionTimeout(Map<String, String> args, int timeout) {
    args.put(TRANSACTION_TIMEOUT, String.valueOf(timeout));
  }

  /**
   * Returns the transction timeout based on the given arguments or, as fallback, the CConfiguration.
   *
   * @returns the integer value of the argument system.data.tx.timeout, or if that is not given in the arguments,
   *          the value for data.tx.timeout from the CConfiguration.
   */
  public static int getTransactionTimeout(Map<String, String> args, CConfiguration cConf) {
    Integer timeout = getPositiveInt(args, TRANSACTION_TIMEOUT, "transaction timeout");
    return timeout != null ? timeout : cConf.getInt(TxConstants.Manager.CFG_TX_TIMEOUT);
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
   * Gets a positive integer value from the given map using the given key.
   * If there is no such key or if the value is negative, returns {@code null}.
   */
  private static Integer getPositiveInt(Map<String, String> map, String key, String description) {
    String value = map.get(key);
    if (value == null) {
      return null;
    }

    try {
      int intValue = Integer.parseInt(value);
      if (intValue <= 0) {
        throw new IllegalArgumentException("Negative " + description + " is not allowed.");
      }
      return intValue;
    } catch (Exception e) {
      // Only the log the stack trace as debug, as usually it's not needed.
      LOG.warn("Ignoring invalid {} '{}' from runtime arguments. It must be a positive integer.", description, value);
      LOG.debug("Invalid {}", description, e);
    }

    return null;
  }

  private SystemArguments() {
  }
}
