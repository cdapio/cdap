/*
 * Copyright © 2021-2023 Cask Data, Inc.
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

package io.cdap.cdap.metrics.jmx;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Metrics.JvmResource;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a service  that runs along with other services to collect resource usage metrics and
 * publish them using a {@link MetricsPublisher}. For this service to work, the jvm process needs to
 * expose JMX server on the same port that this service polls. To do this, the following JAVA OPTS
 * need to be set: {@code -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=11022
 * -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false} The
 * property for setting the JMX server port number should be present in {@code cdap-site.xml}.
 */
public class JmxMetricsCollector extends AbstractScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(JmxMetricsCollector.class);
  private static final long MEGA_BYTE = 1024 * 1024;
  private static final long MAX_PORT = (1 << 16) - 1;
  private static final long SYSTEM_LOAD_SCALING_FACTOR = 100;
  private static final String SERVICE_URL_FORMAT = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";
  private final Map<String, String> metricTags;
  private final CConfiguration cConf;
  private final MetricsPublisher metricsPublisher;
  private final JMXServiceURL serviceUrl;
  private ScheduledExecutorService executor;

  /**
   * Constructs a new {@link JmxMetricsCollector}.
   *
   * @param cConf            The CConf to use.
   * @param metricsPublisher The metrics publisher to use.
   * @param metricTags       The tags to use.
   * @throws MalformedURLException If the server URL is invalid.
   */
  @Inject
  public JmxMetricsCollector(CConfiguration cConf,
      MetricsPublisher metricsPublisher,
      @Assisted Map<String, String> metricTags) throws MalformedURLException {
    this.cConf = cConf;
    int serverPort = cConf.getInt(Constants.JmxMetricsCollector.SERVER_PORT);
    if (serverPort < 0 || serverPort > MAX_PORT) {
      throw new IllegalArgumentException(String.format(
          "%s variable (%d) is not a valid port number.",
          Constants.JmxMetricsCollector.SERVER_PORT, serverPort));
    }
    String serverUrl = String.format(SERVICE_URL_FORMAT, "localhost", serverPort);
    this.metricTags = metricTags;
    this.metricsPublisher = metricsPublisher;
    this.serviceUrl = new JMXServiceURL(serverUrl);
  }

  @Override
  protected void startUp() {
    LOG.info("Starting JmxMetricsCollector with polling frequency: {}, JMX server port: {}",
        this.cConf.getInt(Constants.JmxMetricsCollector.POLL_INTERVAL_SECS),
        this.cConf.getInt(Constants.JmxMetricsCollector.SERVER_PORT));
    this.metricsPublisher.initialize();
  }

  @Override
  protected void shutDown() throws IOException {
    if (executor != null) {
      executor.shutdownNow();
    }
    this.metricsPublisher.close();
    LOG.info("Shutting down JmxMetricsCollector has completed.");
  }

  @Override
  protected void runOneIteration() {
    Collection<MetricValue> metrics = new ArrayList<>();

    try (JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null)) {
      MBeanServerConnection mBeanConn = jmxConnector.getMBeanServerConnection();
      metrics.addAll(getMemoryMetrics(mBeanConn));
      metrics.addAll(getCpuMetrics(mBeanConn));
      metrics.addAll(getThreadMetrics(mBeanConn));
    } catch (IOException e) {
      LOG.error("Error occurred while connecting to JMX server.", e);
      return;
    }
    try {
      this.metricsPublisher.publish(metrics, this.metricTags);
    } catch (Exception e) {
      LOG.warn("Error occurred while publishing resource usage metrics.", e);
    }
  }

  Collection<MetricValue> getMemoryMetrics(MBeanServerConnection mBeanConn) throws IOException {
    MemoryMXBean mxBean = ManagementFactory
        .newPlatformMXBeanProxy(mBeanConn, ManagementFactory.MEMORY_MXBEAN_NAME,
            MemoryMXBean.class);
    MemoryUsage heapMemoryUsage = mxBean.getHeapMemoryUsage();
    Collection<MetricValue> metrics = new ArrayList<>();
    metrics.add(new MetricValue(JvmResource.HEAP_USED_MB,
        MetricType.GAUGE, heapMemoryUsage.getUsed() / MEGA_BYTE));
    if (heapMemoryUsage.getMax() >= 0) {
      metrics.add(new MetricValue(JvmResource.HEAP_MAX_MB,
          MetricType.GAUGE, heapMemoryUsage.getMax() / MEGA_BYTE));
    }
    return metrics;
  }

  Collection<MetricValue> getCpuMetrics(MBeanServerConnection conn) throws IOException {
    OperatingSystemMXBean mxBean = ManagementFactory
        .newPlatformMXBeanProxy(conn, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME,
            OperatingSystemMXBean.class);
    Collection<MetricValue> metrics = new ArrayList<>();
    double systemLoad = mxBean.getSystemLoadAverage();
    if (systemLoad < 0) {
      LOG.info("CPU load for JVM process is not yet available");
    } else {
      double processorCount = mxBean.getAvailableProcessors();
      double systemLoadPerProcessorScaled =
          (systemLoad * SYSTEM_LOAD_SCALING_FACTOR) / processorCount;
      metrics.add(new MetricValue(JvmResource.SYSTEM_LOAD_PER_PROCESSOR_SCALED,
          MetricType.GAUGE, (long) systemLoadPerProcessorScaled));
    }
    return metrics;
  }

  Collection<MetricValue> getThreadMetrics(MBeanServerConnection conn) throws IOException {
    ThreadMXBean mxBean = ManagementFactory
        .newPlatformMXBeanProxy(conn, ManagementFactory.THREAD_MXBEAN_NAME, ThreadMXBean.class);
    Collection<MetricValue> metrics = new ArrayList<>();
    metrics.add(new MetricValue(JvmResource.THREAD_COUNT,
        MetricType.GAUGE, mxBean.getThreadCount()));
    return metrics;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        0, cConf.getInt(Constants.JmxMetricsCollector.POLL_INTERVAL_SECS), TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
        Threads.createDaemonThreadFactory("jmx-metrics-collector"));
    return executor;
  }
}
