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

package io.cdap.cdap.healthcheck;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.app.guice.HealthCheckModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.implementation.HealthCheckImplementation;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link HealthCheckImplementation}. The test is disabled by default since it requires a running
 * kubernetes cluster for the test to run. This class is kept for development purpose.
 */
@Ignore
public class HealthCheckImplementationTest {
  private static final String CDAP_NAMESPACE = "TEST_CDAP_Namespace";
  private static HealthCheckImplementation healthCheckImplementation;
  private static CoreV1Api coreV1Api;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();

    Injector injector =
      Guice.createInjector(new ConfigModule(cConf), new HealthCheckModule());

    healthCheckImplementation = injector.getInstance(HealthCheckImplementation.class);

    coreV1Api = mock(CoreV1Api.class);
  }


  @Test
  public void testSupportBundleK8sHealthCheckTask() throws Exception {
    String podLabelSelectorName = "cdap.container.Logs=bundle-test-v11";
    String nodeFieldSelectorName = "metadata.name=gke-bundle-test-v11-default-pool-42f08219-dt5b";
    HealthCheckResponse healthCheckResponse =
      healthCheckImplementation.collect();

    V1ServiceList v1ServiceList = new V1ServiceList();
    V1Service v1Service = new V1Service();
    V1ObjectMeta v1ObjectMeta = new V1ObjectMeta();
    v1ObjectMeta.setName(Constants.HealthCheck.APP_FABRIC_HEALTH_CHECK_SERVICE);
    v1Service.setMetadata(v1ObjectMeta);
    v1ServiceList.addItemsItem(v1Service);
    when(coreV1Api.listNamespacedPod(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(),
                                     any())).thenReturn(new V1PodList());
    when(coreV1Api.readNamespacedPodLog(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(),
                                        any())).thenReturn("");
    when(coreV1Api.listNode(any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(
      new V1NodeList());
    when(coreV1Api.listNamespacedService(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(),
                                         any())).thenReturn(v1ServiceList);

    Assert.assertNotNull(healthCheckResponse.getData());
  }
}
