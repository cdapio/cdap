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

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.Executor;
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
public class OperationalStatsService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsService.class);
  private static final Logger READ_FAILURE_LOG =
    Loggers.sampling(LOG, LogSamplers.limitRate(TimeUnit.HOURS.toMillis(6)));

  private final OperationalStatsLoader operationalStatsLoader;
  private final long statsRefreshInterval;
  private final Injector injector;

  private Thread runThread;

  @Inject
  OperationalStatsService(CConfiguration cConf, Injector injector) {
    this.operationalStatsLoader = new OperationalStatsLoader(cConf);
    this.statsRefreshInterval = cConf.getLong(Constants.OperationalStats.REFRESH_INTERVAL_SECS);
    this.injector = injector;
  }

  /**
   * Registers all JMX {@link MXBean MXBeans} from {@link OperationalStats} extensions in the extensions directory.
   */
  @Override
  protected void startUp() throws Exception {
    runThread = Thread.currentThread();

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (Map.Entry<OperationalExtensionId, OperationalStats> entry : operationalStatsLoader.getAll().entrySet()) {
      OperationalStats operationalStats = entry.getValue();
      ObjectName objectName = getObjectName(operationalStats);
      if (objectName == null) {
        LOG.warn("Found an operational extension with null service name and stat type - {}. Ignoring this extension.",
                 OperationalStats.class.getName());
        continue;
      }
      LOG.debug("Registering operational extension: {}; extension id: {}", operationalStats, entry.getKey());
      // initialize operational stats
      operationalStats.initialize(injector);
      // register MBean
      mbs.registerMBean(operationalStats, objectName);
    }

    LOG.info("Successfully started Operational Stats Service...");
  }

  @Override
  protected void run() {
    while (isRunning()) {
      try {
        collectOperationalStats();
        if (!isRunning()) {
          // Need to check here before sleep as the collectOperationStats may swallow interrupted exception
          break;
        }
        TimeUnit.SECONDS.sleep(statsRefreshInterval);
      } catch (InterruptedException e) {
        // Expected on stopping. So just break the loop
        break;
      }
    }
  }

  /**
   * Collects stats from all {@link OperationalStats}.
   */
  private void collectOperationalStats() throws InterruptedException {
    LOG.trace("Running operational stats extension service iteration");
    for (Map.Entry<OperationalExtensionId, OperationalStats> entry : operationalStatsLoader.getAll().entrySet()) {
      if (!isRunning()) {
        return;
      }

      OperationalStats stats = entry.getValue();
      LOG.trace("Collecting stats for service {} of type {}", stats.getServiceName(), stats.getStatType());
      try {
        stats.collect();
      } catch (Throwable t) {
        Throwables.propagateIfInstanceOf(t, InterruptedException.class);
        Throwable rootCause = Throwables.getRootCause(t);
        if (rootCause instanceof ServiceUnavailableException || rootCause instanceof TException) {
          // Required service (for example DatasetService in case of ServiceUnavailableException
          // or Transaction Service in case of TException) is not running yet.
          // Return without logging.
          return;
        }
        if (rootCause instanceof InterruptedException) {
          throw (InterruptedException) rootCause;
        }
        READ_FAILURE_LOG.warn("Failed to collect stats for service {} of type {} due to {}",
                              stats.getServiceName(), stats.getStatType(), rootCause.getMessage());
      }
    }
  }

  @Override
  protected Executor executor() {
    final String name = getClass().getSimpleName();
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread thread = new Thread(command, name);
        thread.setDaemon(true);
        thread.start();
      }
    };
  }

  @Override
  protected void shutDown() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (Map.Entry<OperationalExtensionId, OperationalStats> entry : operationalStatsLoader.getAll().entrySet()) {
      OperationalStats operationalStats = entry.getValue();
      ObjectName objectName = getObjectName(operationalStats);
      if (objectName == null) {
        LOG.warn("Found an operational extension with null service name and stat type while unregistering - {}. " +
                   "Ignoring this extension.", operationalStats.getClass().getName());
        continue;
      }
      try {
        mbs.unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOG.warn("MBean {} not found while un-registering. Ignoring.", objectName);
      } catch (MBeanRegistrationException e) {
        LOG.warn("Error while un-registering MBean {}.", e);
      }
      operationalStats.destroy();
    }
    LOG.info("Successfully shutdown operational stats service.");
  }

  @Override
  protected void triggerShutdown() {
    if (runThread != null) {
      runThread.interrupt();
    }
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
