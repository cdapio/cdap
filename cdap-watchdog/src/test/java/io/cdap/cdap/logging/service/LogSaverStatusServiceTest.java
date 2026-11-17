/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.service;

import com.google.common.collect.Iterables;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import java.util.Collections;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link LogSaverStatusService} {@code toString()} format and lifecycle.
 */
public class LogSaverStatusServiceTest {

  @Test
  public void testToStringAndLifecycle() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.LogSaver.ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.LogSaver.PORT, 0);

    InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
    CommonNettyHttpServiceFactory httpFactory = new CommonNettyHttpServiceFactory(
        cConf, new NoOpMetricsCollectionService(), entry -> { }, null);
    LogSaverStatusService service = new LogSaverStatusService(
        cConf, SConfiguration.create(), discoveryService, Collections.emptySet(), httpFactory);

    Assert.assertEquals("LogSaverStatusService{bindAddress=/127.0.0.1:0}", service.toString());

    service.startAsync().awaitRunning();
    Discoverable discovered =
        Iterables.getOnlyElement(discoveryService.discover(Constants.Service.LOGSAVER));
    Assert.assertEquals("LogSaverStatusService{bindAddress=" + discovered.getSocketAddress() + "}",
        service.toString());

    service.stopAsync().awaitTerminated();
    Assert.assertTrue(Iterables.isEmpty(discoveryService.discover(Constants.Service.LOGSAVER)));
  }
}
