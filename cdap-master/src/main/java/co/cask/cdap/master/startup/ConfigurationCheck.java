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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import kafka.common.Topic;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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
    checkPotentialPortConflicts();
    checkKafkaTopic(problemKeys);

    if (!problemKeys.isEmpty()) {
      throw new RuntimeException("Invalid configuration settings for keys: " + Joiner.on(',').join(problemKeys));
    }
    LOG.info("  Configuration successfully verified.");
  }

  // checks that instances, max instances, memory, and vcores for system services are positive integers,
  // and the instances does not exceed max instances
  private void checkServiceResources(Set<String> problemKeys) {
    for (ServiceResourceKeys serviceResourceKeys : systemServicesResourceKeys) {
      // verify memory and vcores are positive integers
      if (!isPositiveInteger(serviceResourceKeys.getMemoryKey())) {
        LOG.error("  {} must be a positive integer", serviceResourceKeys.getMemoryKey());
        problemKeys.add(serviceResourceKeys.getMemoryKey());
      }
      if (!isPositiveInteger(serviceResourceKeys.getVcoresKey())) {
        LOG.error("  {} must be a positive integer", serviceResourceKeys.getVcoresKey());
        problemKeys.add(serviceResourceKeys.getVcoresKey());
      }

      // verify instances and max instances are positive integers
      boolean instancesIsPositive = isPositiveInteger(serviceResourceKeys.getInstancesKey());
      boolean maxInstancesIsPositive = isPositiveInteger(serviceResourceKeys.getMaxInstancesKey());
      if (!instancesIsPositive) {
        LOG.error("  {} must be a positive integer", serviceResourceKeys.getInstancesKey());
        problemKeys.add(serviceResourceKeys.getInstancesKey());
      }
      if (!maxInstancesIsPositive) {
        LOG.error("  {} must be a positive integer", serviceResourceKeys.getMaxInstancesKey());
        problemKeys.add(serviceResourceKeys.getMaxInstancesKey());
      }

      // verify instances <= maxInstances
      if (instancesIsPositive && maxInstancesIsPositive) {
        int instances = cConf.getInt(serviceResourceKeys.getInstancesKey());
        int maxInstances = cConf.getInt(serviceResourceKeys.getMaxInstancesKey());
        if (instances > maxInstances) {
          LOG.error("  {}={} must not be greater than {}={}",
                    serviceResourceKeys.getInstancesKey(), instances,
                    serviceResourceKeys.getMaxInstancesKey(), maxInstances);
          problemKeys.add(serviceResourceKeys.getInstancesKey());
        }
      }
    }
  }

  private void checkPotentialPortConflicts() {
    // check for potential port conflicts
    Multimap<Integer, String> services = HashMultimap.create();
    if (cConf.getBoolean(Constants.Security.SSL_ENABLED)) {
      services.put(cConf.getInt(Constants.Router.ROUTER_SSL_PORT), "Router");
      services.put(cConf.getInt(Constants.Router.WEBAPP_SSL_PORT), "UI");
      services.put(cConf.getInt(Constants.Security.AuthenticationServer.SSL_PORT), "Authentication Server");
    } else {
      services.put(cConf.getInt(Constants.Router.ROUTER_PORT), "Router");
      services.put(cConf.getInt(Constants.Router.WEBAPP_PORT), "UI");
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
    if (!isValidKafkaTopic(Constants.Notification.KAFKA_TOPIC)) {
      problemKeys.add(Constants.Notification.KAFKA_TOPIC);
    }
    if (!isValidKafkaTopic(Constants.Logging.KAFKA_TOPIC)) {
      problemKeys.add(Constants.Logging.KAFKA_TOPIC);
    }
    if (!isValidKafkaTopic(Constants.Metadata.UPDATES_KAFKA_TOPIC)) {
      problemKeys.add(Constants.Metadata.UPDATES_KAFKA_TOPIC);
    }
  }

  private boolean isPositiveInteger(String key) {
    try {
      return cConf.getInt(key) > 0;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isValidKafkaTopic(String key) {
    try {
      Topic.validate(cConf.get(key));
    } catch (InvalidTopicException e) {
      LOG.error("  {} key has invalid kafka topic name. {}", key, e.getMessage());
      return false;
    }
    return true;
  }
}
