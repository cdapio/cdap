/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.healthcheck;

import com.google.gson.JsonArray;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.HealthCheckModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.implementation.HealthCheckImplementation;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HealthCheckImplementationTest {
  private static HealthCheckImplementation healthCheckImplementation;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();

    Injector injector =
      Guice.createInjector(new ConfigModule(cConf), new HealthCheckModule(), new InMemoryDiscoveryModule());
    JsonArray podInfoList = new JsonArray();
    JsonArray nodeInfoList = new JsonArray();
    JsonArray eventInfoList = new JsonArray();

    cConf.set(Constants.AppFabricHealthCheck.POD_INFO, podInfoList.toString());
    cConf.set(Constants.AppFabricHealthCheck.NODE_INFO, nodeInfoList.toString());
    cConf.set(Constants.AppFabricHealthCheck.EVENT_INFO, eventInfoList.toString());
    cConf.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_POD, Constants.AppFabricHealthCheck.POD_INFO);
    cConf.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_NODE, Constants.AppFabricHealthCheck.NODE_INFO);
    cConf.set(Constants.AppFabricHealthCheck.SERVICE_NAME_WITH_EVENT, Constants.AppFabricHealthCheck.EVENT_INFO);

    healthCheckImplementation = injector.getInstance(HealthCheckImplementation.class);
  }


  @Test
  public void testSupportBundleK8sHealthCheckTask() throws Exception {
    HealthCheckResponse healthCheckResponse =
      healthCheckImplementation.collect(Constants.AppFabricHealthCheck.APP_FABRIC_HEALTH_CHECK_SERVICE);
    Assert.assertNotNull(healthCheckResponse.getData());
  }
}
