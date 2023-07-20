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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.metrics.MetricsPublisher;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.rmi.registry.LocateRegistry;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link JmxMetricsCollector}.
 */
public class JmxMetricsCollectorTest {

  private static final int SERVER_PORT = 11023;
  private static JMXConnectorServer svr;
  @Mock
  private MetricsPublisher publisher;

  @BeforeClass
  public static void setupClass() throws IOException {
    svr = createJmxConnectorServer(SERVER_PORT);
    svr.start();
    Assert.assertTrue(svr.isActive());
  }

  private static JMXConnectorServer createJmxConnectorServer(int port) throws IOException {
    LocateRegistry.createRegistry(port);
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    JMXServiceURL url = new JMXServiceURL(
        String.format("service:jmx:rmi://localhost/jndi/rmi://localhost:%d/jmxrmi", port));
    return JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    svr.stop();
  }

  @Before
  public void beforeEach() {
    MockitoAnnotations.initMocks(this);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPortInConfig() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.JmxMetricsCollector.SERVER_PORT, -1);
    cConf.setInt(Constants.JmxMetricsCollector.POLL_INTERVAL_SECS, 1);
    Map<String, String> metricTags = ImmutableMap.of("key1", "value1", "key2", "value2");
    new JmxMetricsCollector(cConf, publisher, metricTags);
  }

  @Test
  public void testNumberOfMetricsEmitted() throws InterruptedException, MalformedURLException,
      ExecutionException, TimeoutException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constants.JmxMetricsCollector.SERVER_PORT, SERVER_PORT);
    cConf.setInt(Constants.JmxMetricsCollector.POLL_INTERVAL_SECS, 1);
    Map<String, String> metricTags = ImmutableMap.of("key1", "value1", "key2", "value2");
    JmxMetricsCollector jmxMetrics = new JmxMetricsCollector(cConf, publisher, metricTags);
    jmxMetrics.startAndWait();
    verify(publisher, times(1)).initialize();
    // Poll should run at 0, 1. 2 secs buffer.
    Tasks.waitFor(true, () -> {
      try {
        verify(publisher, atLeast(1)).publish(any(), any());
      } catch (Throwable e) {
        return false;
      }
      return true;
    }, 3, TimeUnit.SECONDS);
    jmxMetrics.stop();
  }
}
