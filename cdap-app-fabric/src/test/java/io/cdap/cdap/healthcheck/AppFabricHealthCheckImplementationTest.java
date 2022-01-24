/*
 * Copyright © 2021 Cask Data, Inc.
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
package io.cdap.cdap.healthcheck;

import com.google.gson.JsonArray;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.healthcheck.implementation.AppFabricHealthCheckImplementation;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.rmi.registry.LocateRegistry;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import static io.cdap.cdap.internal.AppFabricTestHelper.getInjector;

public class AppFabricHealthCheckImplementationTest {
  private static final int SERVER_PORT = 11023;
  private static CConfiguration configuration;
  private static AppFabricHealthCheckImplementation appFabricHealthCheckImplementation;
  private static JMXConnectorServer svr;

  @BeforeClass
  public static void setup() throws Exception {
    Injector injector = getInjector();
    JsonArray podInfoList = new JsonArray();
    JsonArray nodeInfoList = new JsonArray();
    JsonArray eventInfoList = new JsonArray();
    configuration = injector.getInstance(CConfiguration.class);
    configuration.set(Constants.AppFabricHealthCheck.POD_INFO, podInfoList.toString());
    configuration.set(Constants.AppFabricHealthCheck.NODE_INFO, nodeInfoList.toString());
    configuration.set(Constants.AppFabricHealthCheck.EVENT_INFO, eventInfoList.toString());
    configuration.setInt(Constants.JMXMetricsCollector.SERVER_PORT, SERVER_PORT);
    configuration.setInt(Constants.JMXMetricsCollector.POLL_INTERVAL_SECS, 1);

    appFabricHealthCheckImplementation = injector.getInstance(AppFabricHealthCheckImplementation.class);
    svr = createJMXConnectorServer(SERVER_PORT);
    svr.start();
    Assert.assertTrue(svr.isActive());
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    svr.stop();
  }

  private static JMXConnectorServer createJMXConnectorServer(int port) throws IOException {
    LocateRegistry.createRegistry(port);
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    JMXServiceURL url =
      new JMXServiceURL(String.format("service:jmx:rmi://localhost/jndi/rmi://localhost:%d/jmxrmi", port));
    return JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
  }

  @Test
  public void testSupportBundleK8sHealthCheckTask() throws Exception {
    HealthCheckResponse healthCheckResponse = appFabricHealthCheckImplementation.collect();
    Assert.assertNotNull(healthCheckResponse.getData());
  }
}
