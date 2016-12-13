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

package co.cask.cdap.operations;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * A service that registers {@link OperationalStats} extensions as JMX Beans. The Beans are registered with the JMX
 * domain {@link OperationalStatsUtils#JMX_DOMAIN} with the following properties:
 * <ol>
 *   <li>{@link OperationalStatsUtils#SERVICE_NAME_KEY} = name of the service, as defined by
 *   {@link OperationalStats#getServiceName()};</li>
 *   <li>{@link OperationalStatsUtils#STAT_TYPE_KEY} = type of the stat; as defined by
 *   {@link OperationalStats#getStatType()}</li>
 * </ol>
 *
 * It also updates the Beans periodically by calling their {@link OperationalStats#collect()} method.
 */
public class OperationalStatsService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsService.class);

  private final OperationalStatsLoader operationalStatsLoader;
  private final int statsRefreshInterval;

  private ScheduledExecutorService executor;

  @Inject
  OperationalStatsService(OperationalStatsLoader operationalStatsLoader, CConfiguration cConf) {
    this.operationalStatsLoader = operationalStatsLoader;
    this.statsRefreshInterval = cConf.getInt(Constants.OperationalStats.REFRESH_INTERVAL_SECS);
  }

  /**
   * Registers all JMX {@link MXBean MXBeans} from {@link OperationalStats} extensions in the extensions directory.
   */
  @Override
  protected void startUp() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (Map.Entry<OperationalExtensionId, OperationalStats> entry : operationalStatsLoader.getAll().entrySet()) {
      ObjectName objectName = getObjectName(entry.getValue());
      if (objectName == null) {
        LOG.warn("Found an operational extension with null service name and stat type - {}. Ignoring this extension.",
                 OperationalStats.class.getName());
        continue;
      }
      LOG.debug("Registering operational extension: {}; extension id: {}", entry.getValue(), entry.getKey());
      // register MBean
      mbs.registerMBean(entry.getValue(), objectName);
    }
    LOG.info("Successfully started Operational Stats Service...");
  }

  /**
   * Also schedules asynchronous stats collection for all {@link MXBean MXBeans} by calling the
   * {@link OperationalStats#collect()} method.
   */
  @Override
  protected void runOneIteration() throws Exception {
    LOG.debug("Running operational stats extension service iteration");
    for (Map.Entry<OperationalExtensionId, OperationalStats> entry : operationalStatsLoader.getAll().entrySet()) {
      LOG.debug("Collecting {] stats for service {}", entry.getValue().getStatType(),
                entry.getValue().getServiceName());
      try {
        entry.getValue().collect();
      } catch (Throwable t) {
        LOG.warn("Error while collecting stats for service: {}; type: {}", entry.getValue().getServiceName(),
                 entry.getValue().getStatType(), t);
      }
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, statsRefreshInterval, TimeUnit.SECONDS);
  }

  @Override
  protected ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("operational-stats-collector-%d"));
    return executor;
  }

  @Override
  protected void shutDown() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (Map.Entry<OperationalExtensionId, OperationalStats> entry : operationalStatsLoader.getAll().entrySet()) {
      ObjectName objectName = getObjectName(entry.getValue());
      if (objectName == null) {
        LOG.warn("Found an operational extension with null service name and stat type while unregistering - {}. " +
                   "Ignoring this extension.", entry.getValue().getClass().getName());
        continue;
      }
      try {
        mbs.unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOG.debug("MBean {} not found while un-registering. Ignoring.", objectName);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Error while un-registering MBean {}.", e);
      }
    }
    if (executor != null) {
      executor.shutdownNow();
    }
    LOG.info("Successfully shutdown operational stats service.");
  }

  @Nullable
  private ObjectName getObjectName(OperationalStats operationalStats) {
    OperationalExtensionId operationalExtensionId = OperationalStatsUtils.getOperationalExtensionId(operationalStats);
    if (operationalExtensionId == null) {
      return null;
    }
    Hashtable<String, String> properties = new Hashtable<>();
    properties.put(OperationalStatsUtils.SERVICE_NAME_KEY, operationalExtensionId.getServiceName());
    properties.put(OperationalStatsUtils.STAT_TYPE_KEY, operationalExtensionId.getStatType());
    try {
      return new ObjectName(OperationalStatsUtils.JMX_DOMAIN, properties);
    } catch (MalformedObjectNameException e) {
      // should never happen, since we're constructing a valid domain name, and properties is non-empty
      throw Throwables.propagate(e);
    }
  }
}
