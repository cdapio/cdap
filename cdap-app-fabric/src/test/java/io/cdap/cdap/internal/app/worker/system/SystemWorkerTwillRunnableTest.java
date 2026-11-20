/*
 * Copyright © 2022-2026 Cask Data, Inc.
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

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.encryption.NoOpAeadCipher;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.internal.remote.RunnableTaskModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.distributed.MockMasterEnvironment;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.guice.FileBasedCoreSecurityModule;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SystemWorkerTwillRunnable}.
 */
public class SystemWorkerTwillRunnableTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testInjector() {
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    Injector injector = SystemWorkerTwillRunnable
        .createInjector(CConfiguration.create(), new Configuration(), SConfiguration.create());
    injector.getInstance(ArtifactManagerFactory.class).create(NamespaceId.SYSTEM, RetryStrategies.noRetry());
  }

  @Test
  public void testDoInitialize() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.SystemWorker.LOCAL_DATA_DIR, temporaryFolder.newFolder().getAbsolutePath());
    File cConfFileTemp = temporaryFolder.newFile("cConf.xml");
    File hConfFileTemp = temporaryFolder.newFile("hConf.xml");
    File sConfFileTemp = temporaryFolder.newFile("sConf.xml");
    try (FileOutputStream fileOutputStream = new FileOutputStream(cConfFileTemp.getAbsolutePath())) {
      cConf.writeXml(fileOutputStream);
    }
    try (FileOutputStream fileOutputStream = new FileOutputStream(hConfFileTemp.getAbsolutePath())) {
      new io.cdap.cdap.common.conf.Configuration().writeXml(fileOutputStream);
    }
    try (FileOutputStream fileOutputStream = new FileOutputStream(sConfFileTemp.getAbsolutePath())) {
      new io.cdap.cdap.common.conf.Configuration().writeXml(fileOutputStream);
    }

    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    SystemWorkerTwillRunnable systemWorkerTwillRunnable = new SystemWorkerTwillRunnable(
        cConfFileTemp.getAbsolutePath(), hConfFileTemp.getAbsolutePath(), sConfFileTemp.getAbsolutePath());
    systemWorkerTwillRunnable.doInitialize();
    systemWorkerTwillRunnable.stop();
    systemWorkerTwillRunnable.destroy();
  }

  @Test
  public void testInitializePropagatesRuntimeExceptionUnwrapped() {
    SystemWorkerTwillRunnable runnable =
        new SystemWorkerTwillRunnable("missing-cConf.xml", "missing-hConf.xml", "missing-sConf.xml");
    TwillContext context = Mockito.mock(TwillContext.class);
    Mockito.when(context.getSpecification()).thenReturn(null);
    try {
      runnable.initialize(context);
      Assert.fail("Expected NullPointerException");
    } catch (NullPointerException expected) {
      Assert.assertNull(expected.getCause());
    }
  }

  @Test
  public void testRunStopAndDestroyLifecycle() throws Exception {
    File keyFile = new File(temporaryFolder.newFolder(), "key");
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
    MetricsCollectionService metrics = new NoOpMetricsCollectionService();
    metrics.startAndWait();

    InMemoryDiscoveryService discovery = new InMemoryDiscoveryService();
    SystemWorkerService workerService = new SystemWorkerService(
        cConf,
        SConfiguration.create(),
        discovery,
        metrics,
        new CommonNettyHttpServiceFactory(cConf, metrics, contexts -> { }, new NoOpAeadCipher()),
        securityInjector.getInstance(TokenManager.class),
        new NoopTwillRunnerService(),
        new NoopTwillRunnerService(),
        Mockito.mock(ProvisioningService.class),
        Guice.createInjector(new RunnableTaskModule(discovery, discovery, metrics)),
        new AuthenticationTestContext(),
        new NoOpAccessController());

    SystemWorkerTwillRunnable runnable = new SystemWorkerTwillRunnable("c.xml", "h.xml", "s.xml");
    LogAppenderInitializer logInitializer = Mockito.mock(LogAppenderInitializer.class);

    Field workerField = SystemWorkerTwillRunnable.class.getDeclaredField("systemWorker");
    workerField.setAccessible(true);
    workerField.set(runnable, workerService);

    Field metricsField = SystemWorkerTwillRunnable.class.getDeclaredField("metricsCollectionService");
    metricsField.setAccessible(true);
    metricsField.set(runnable, metrics);

    Field logField = SystemWorkerTwillRunnable.class.getDeclaredField("logAppenderInitializer");
    logField.setAccessible(true);
    logField.set(runnable, logInitializer);

    CountDownLatch runFinished = new CountDownLatch(1);
    Thread runThread = new Thread(() -> {
      try {
        runnable.run();
      } finally {
        runFinished.countDown();
      }
    });
    runThread.start();

    long deadline = System.currentTimeMillis() + 5000L;
    while (workerService.state() != Service.State.RUNNING && System.currentTimeMillis() < deadline) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
    Assert.assertEquals(Service.State.RUNNING, workerService.state());

    runnable.stop();
    Assert.assertTrue(runFinished.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(Service.State.TERMINATED, workerService.state());
    runnable.destroy();
    Mockito.verify(logInitializer).close();
  }
}
