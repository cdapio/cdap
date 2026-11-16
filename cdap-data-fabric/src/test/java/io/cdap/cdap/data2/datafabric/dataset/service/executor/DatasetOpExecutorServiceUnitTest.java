/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.data2.datafabric.dataset.service.executor;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import java.util.Collections;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct fast unit test for {@link DatasetOpExecutorService#toString()} and lifecycle.
 */
public class DatasetOpExecutorServiceUnitTest {

  @Test
  public void testStartStopAndToString() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.Executor.ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.Dataset.Executor.PORT, 0);

    DatasetOpExecutorService service = new DatasetOpExecutorService(
        cConf,
        SConfiguration.create(),
        new InMemoryDiscoveryService(),
        new CommonNettyHttpServiceFactory(
            cConf, new NoOpMetricsCollectionService(), entry -> { }, null),
        Collections.emptySet());

    Assert.assertEquals("DatasetOpExecutorService{bindAddress=/127.0.0.1:0}", service.toString());

    Assert.assertEquals(Service.State.RUNNING, service.startAndWait());
    try {
      int boundPort = service.getHttpService().getBindAddress().getPort();
      Assert.assertTrue(boundPort > 0);
      String runningStr = service.toString();
      Assert.assertTrue("Unexpected toString: " + runningStr,
          runningStr.startsWith("DatasetOpExecutorService{bindAddress="));
      Assert.assertTrue(runningStr.contains(String.valueOf(boundPort)));
    } finally {
      Assert.assertEquals(Service.State.TERMINATED, service.stopAndWait());
    }
  }
}
