/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.appender.kafka.LogPartitionType;
import co.cask.cdap.proto.id.EntityId;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import kafka.common.Topic;
import org.apache.tephra.TxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Checks the CDAP Configuration for bad settings.
 */
// class is picked up through classpath examination
@SuppressWarnings("unused")
class ConfigurationCheck extends AbstractMasterCheck {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationCheck.class);

  @Inject
  private ConfigurationCheck(CConfiguration cConf) {
    super(cConf);
  }

  // TODO: (CDAP-4517) add more checks, like zookeeper settings.
  @Override
  public void run() {
    LOG.info("Checking that config settings are valid.");

    Set<String> problemKeys = new HashSet<>();
    checkServiceResources(problemKeys);
    checkBindAddresses();
    checkPotentialPortConflicts(problemKeys);
    checkKafkaTopic(problemKeys);
    checkMessagingTopics(problemKeys);
    checkLogPartitionKey(problemKeys);
    checkPruningAndReplication(problemKeys);

    if (!problemKeys.isEmpty()) {
      throw new RuntimeException("Invalid configuration settings for keys: " + Joiner.on(',').join(problemKeys));
    }
    LOG.info("  Configuration successfully verified.");
  }

  // tx invalid list pruning is not allowed with replication
  private void checkPruningAndReplication(Set<String> problemKeys) {
    String hbaseDDLExtensionDir = cConf.get(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
    boolean pruningEnabled = cConf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE);
    if (hbaseDDLExtensionDir != null && pruningEnabled) {
      LOG.error("  Invalid transaction list cannot be automatically pruned when replication is in use. " +
                  "Please disable pruning by setting {} to false, or remove your custom HBase DDL executor from {}.",
                TxConstants.TransactionPruning.PRUNE_ENABLE, Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
      problemKeys.add(Constants.HBaseDDLExecutor.EXTENSIONS_DIR);
      problemKeys.add(TxConstants.TransactionPruning.PRUNE_ENABLE);
    }
  }

  // checks that instances, max instances, memory, and vcores for system services are positive integers,
  // and the instances does not exceed max instances
  private void checkServiceResources(Set<String> problemKeys) {
    for (ServiceResourceKeys serviceResourceKeys : systemServicesResourceKeys) {
      validatePositiveInteger(serviceResourceKeys.getMemoryKey(), problemKeys);
      validatePositiveInteger(serviceResourceKeys.getVcoresKey(), problemKeys);
      Integer instances = validatePositiveInteger(serviceResourceKeys.getInstancesKey(), problemKeys);
      Integer maxInstances = validatePositiveInteger(serviceResourceKeys.getMaxInstancesKey(), problemKeys);

      // verify instances <= maxInstances
      if (instances != null && maxInstances != null &&  instances > maxInstances) {
        LOG.error("  {} is set to {} but must not be greater than the {} of {}",
                  serviceResourceKeys.getInstancesKey(), instances,
                  serviceResourceKeys.getMaxInstancesKey(), maxInstances);
        problemKeys.add(serviceResourceKeys.getInstancesKey());
      }
    }
  }

  private void checkBindAddresses() {
    // check if service bind addresses are loopback addresses
    Set<String> bindAddressKeys = ImmutableSet.of(Constants.Service.MASTER_SERVICES_BIND_ADDRESS,
                                                  Constants.Router.ADDRESS);

    for (String bindAddressKey : bindAddressKeys) {
      String bindAddress = cConf.get(bindAddressKey);
      try {
        if (InetAddress.getByName(bindAddress).isLoopbackAddress()) {
          LOG.warn("  {} is set to {}. The service may not be discoverable on a multinode Hadoop cluster.",
                   bindAddressKey, bindAddress);
        }
      } catch (UnknownHostException e) {
        LOG.warn("  {} is set to {} and cannot be resolved. ", bindAddressKey, bindAddress, e);
      }
    }
  }

  private void checkPotentialPortConflicts(Set<String> problemKeys) {
    // check for potential port conflicts
    Multimap<Integer, String> services = HashMultimap.create();
    String sslKey = Constants.Security.SSL.EXTERNAL_ENABLED;
    boolean isSSL;
    try {
      isSSL = cConf.getBoolean(sslKey);
    } catch (Exception e) {
      logProblem("  {} is set to {} and cannot be parsed as a boolean", sslKey, cConf.get(sslKey), e);
      problemKeys.add(Constants.Security.SSL.EXTERNAL_ENABLED);
      return;
    }
    if (isSSL) {
      services.put(cConf.getInt(Constants.Router.ROUTER_SSL_PORT), "Router");
      services.put(cConf.getInt(Constants.Security.AuthenticationServer.SSL_PORT), "Authentication Server");
    } else {
      services.put(cConf.getInt(Constants.Router.ROUTER_PORT), "Router");
      services.put(cConf.getInt(Constants.Security.AUTH_SERVER_BIND_PORT), "Authentication Server");
    }
    for (Integer port : services.keySet()) {
      Collection<String> conflictingServices = services.get(port);
      if (conflictingServices.size() > 1) {
        LOG.warn("Potential conflict on port {} for the following services: {}",
                 port, Joiner.on(", ").join(conflictingServices));
      }
    }
  }

  private void checkKafkaTopic(Set<String> problemKeys) {
    validateKafkaTopic(Constants.Logging.KAFKA_TOPIC, problemKeys);
  }

  private void checkLogPartitionKey(Set<String> problemKeys) {
    validatePartitionKey(Constants.Logging.LOG_PUBLISH_PARTITION_KEY, problemKeys);
  }

  private void checkMessagingTopics(Set<String> problemKeys) {
    validateMessagingTopic(Constants.Audit.TOPIC, problemKeys);
    validateMessagingTopic(Constants.Notification.TOPIC, problemKeys);
  }

  /**
   * Validate that the value for the given key is a valid messaging topic, that is, a valid dataset name.
   * If it is not, log an error and add the key to the problemKeys.
   */
  private void validateMessagingTopic(String key, Set<String> problemKeys) {
    String value = cConf.get(key);
    try {
      if (!EntityId.isValidDatasetId(value)) {
        LOG.error("  {} must be a valid entity id but is {}", key, value);
        problemKeys.add(key);
      }
    } catch (Exception e) {
      logProblem("  {} is set to {} and cannot be verified as a valid entity id", key, value, e);
      problemKeys.add(key);
    }
  }

  /**
   * Validate that the value for the given key is a positive integer.
   * If it is not, log an error and add the key to the problemKeys.
   *
   * @return the value configured for the key if it is a positive integer; null otherwise
   */
  private Integer validatePositiveInteger(String key, Set<String> problemKeys) {
    // it may happen that a service does not have this config.
    // for example, explore service does not have instances and maxInstances
    if (key == null) {
      return null;
    }
    String value = cConf.get(key);
    try {
      int intValue = Integer.parseInt(value);
      if (intValue > 0) {
        return intValue;
      }
      LOG.error("  {} must be a positive integer but is {}", key, value);
      problemKeys.add(key);
    } catch (Exception e) {
      logProblem("  {} is set to {} and cannot be parsed as an integer", key, value, e);
      problemKeys.add(key);
    }
    return null;
  }

  /**
   * Validate that the value for the given key is a valid Kafka topic.
   * If it is not, log an error and add the key to the problemKeys.
   */
  private void validateKafkaTopic(String key, Set<String> problemKeys) {
    try {
      Topic.validate(cConf.get(key));
    } catch (Exception e) {
      logProblem("  {} must be a valid kafka topic name but is {}", key, cConf.get(key), e);
      problemKeys.add(key);
    }
  }

  /**
   * Validate that the value for the given key is a valid partition key.
   * If it is not, log an error and add the key to the problemKeys.
   */
  private void validatePartitionKey(String key, Set<String> problemKeys) {
    try {
      LogPartitionType.valueOf(cConf.get(key).toUpperCase());
    } catch (Exception e) {
      logProblem("  {} must be a valid log partition type (program or application) but is {}", key, cConf.get(key), e);
      problemKeys.add(key);
    }
  }

  /**
   * Best effort to log a user-friendly error message for a badly configured key and value.
   *
   * @param message The message desxcribing the problem
   * @param e The exception that caused the problem.
   */
  private void logProblem(String message, String key, @Nullable String value, @Nullable Exception e) {
    if (e == null || value == null) {
      LOG.error(message, key, value);
    } else if (e.getMessage() == null) {
      LOG.error(message + ". {}", key, value, e.getClass().getSimpleName());
    } else if (e instanceof IllegalArgumentException) {
      LOG.error(message + ". {}", key, value, e.getMessage());
    } else {
      LOG.error(message + ". {}: {}", key, value, e.getClass().getSimpleName(), e.getMessage());
    }
  }
}
