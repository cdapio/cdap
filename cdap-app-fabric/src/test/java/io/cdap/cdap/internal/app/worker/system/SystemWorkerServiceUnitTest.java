/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.system;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.internal.remote.RunnableTaskModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.guice.FileBasedCoreSecurityModule;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link SystemWorkerService} lifecycle without {@code AppFabricTestBase}.
 */
public class SystemWorkerServiceUnitTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStartWaitsForTokenManagerAndStopStopsTokenManager() throws Exception {
    File keyFile = new File(TEMP_FOLDER.newFolder(), "key");
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.SystemWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.SystemWorker.PORT, 0);
    cConf.setBoolean(Constants.Security.SSL.INTERNAL_ENABLED, false);
    cConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, keyFile.getAbsolutePath());

    Injector securityInjector = Guice.createInjector(
        new IOModule(),
        new ConfigModule(cConf),
        new FileBasedCoreSecurityModule(),
        new InMemoryDiscoveryModule());
    TokenManager tokenManager = securityInjector.getInstance(TokenManager.class);
    List<String> lifecycleEvents = Collections.synchronizedList(new ArrayList<>());
    tokenManager.addListener(new Service.Listener() {
      @Override
      public void running() {
        lifecycleEvents.add("tokenManager.running");
      }

      @Override
      public void terminated(Service.State from) {
        lifecycleEvents.add("tokenManager.terminated");
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    ProvisioningService provisioningService = Mockito.mock(ProvisioningService.class);
    Mockito.doAnswer(invocation -> {
      lifecycleEvents.add("provisioningService.init");
      return null;
    }).when(provisioningService).initializeProvisionersAndExecutors();
    NoOpMetricsCollectionService metrics = new NoOpMetricsCollectionService();
    InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();

    SystemWorkerService service = new SystemWorkerService(
        cConf,
        SConfiguration.create(),
        discovery,
        metrics,
        new CommonNettyHttpServiceFactory(cConf, metrics, contexts -> { }),
        tokenManager,
        new NoopTwillRunnerService(),
        new NoopTwillRunnerService(),
        provisioningService,
        Guice.createInjector(new RunnableTaskModule(discovery, discovery, metrics)),
        new AuthenticationTestContext(),
        new NoOpAccessController());

    service.startAsync().awaitRunning();
    Assert.assertEquals(Service.State.RUNNING, service.state());
    Assert.assertEquals(Service.State.RUNNING, tokenManager.state());
    Assert.assertNotNull(service.getBindAddress());
    Assert.assertEquals(
        ImmutableList.of("tokenManager.running", "provisioningService.init"),
        lifecycleEvents);

    service.stopAsync().awaitTerminated();
    Assert.assertEquals(Service.State.TERMINATED, service.state());
    Assert.assertEquals(Service.State.TERMINATED, tokenManager.state());
    Assert.assertEquals(
        ImmutableList.of(
            "tokenManager.running", "provisioningService.init", "tokenManager.terminated"),
        lifecycleEvents);
  }
}
