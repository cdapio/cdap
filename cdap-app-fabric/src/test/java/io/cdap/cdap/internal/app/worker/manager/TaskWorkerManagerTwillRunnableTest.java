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

package io.cdap.cdap.internal.app.worker.manager;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.distributed.MockMasterEnvironment;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.master.environment.MasterEnvironments;
import java.io.File;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit test for {@link TaskWorkerManagerTwillRunnable}.
 */
public class TaskWorkerManagerTwillRunnableTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    MasterEnvironments.setMasterEnvironment(null);
  }

  /**
   * Catches missing bindings in the manager's minimal module set before they fail a pod at startup.
   */
  @Test
  public void testInjector() {
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());

    Injector injector = TaskWorkerManagerTwillRunnable.createInjector(CConfiguration.create(),
        new Configuration());

    Assert.assertNotNull(injector.getInstance(LogAppenderInitializer.class));
    // One service per pod: it owns the lease table and the listening port.
    TaskWorkerManagerService service = injector.getInstance(TaskWorkerManagerService.class);
    Assert.assertNotNull(service);
    Assert.assertSame(service, injector.getInstance(TaskWorkerManagerService.class));
  }

  /**
   * The manager gets the per-pod client, and everything else keeps the ClusterIP client.
   */
  @Test
  public void testInjectorBindsBothDiscoveryClients() {
    MockMasterEnvironment masterEnv = new MockMasterEnvironment();
    MasterEnvironments.setMasterEnvironment(masterEnv);

    Injector injector = TaskWorkerManagerTwillRunnable.createInjector(CConfiguration.create(),
        new Configuration());

    Assert.assertSame(masterEnv.getEndpointsDiscoveryServiceClientSupplier().get(),
        injector.getInstance(Key.get(DiscoveryServiceClient.class,
            Names.named(TaskWorkerManagerService.WORKER_DISCOVERY))));
    Assert.assertSame(masterEnv.getDiscoveryServiceClientSupplier().get(),
        injector.getInstance(DiscoveryServiceClient.class));
  }

  /**
   * Without a master environment the injector fails instead of falling back to ZooKeeper.
   */
  @Test
  public void testInjectorRequiresMasterEnvironment() {
    MasterEnvironments.setMasterEnvironment(null);

    try {
      TaskWorkerManagerTwillRunnable.createInjector(CConfiguration.create(), new Configuration());
      Assert.fail("Expected IllegalStateException without a MasterEnvironment");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("MasterEnvironment"));
    }
  }

  @Test
  public void testStopBeforeRunCompletesWithoutHanging() throws Exception {
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorkerManager.ADDRESS, "127.0.0.1");
    cConf.setInt(Constants.TaskWorkerManager.PORT, 0);
    TaskWorkerManagerTwillRunnable runnable = initializedRunnable(cConf);
    try {
      runnable.stop();
      CompletableFuture<Void> runFuture = CompletableFuture.runAsync(runnable::run);
      runFuture.get(10, TimeUnit.SECONDS);
    } finally {
      runnable.destroy();
    }
  }

  @Test
  public void testRunPropagatesServiceStartupFailure() throws Exception {
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    try (ServerSocket occupied = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      CConfiguration cConf = CConfiguration.create();
      cConf.set(Constants.TaskWorkerManager.ADDRESS, "127.0.0.1");
      cConf.setInt(Constants.TaskWorkerManager.PORT, occupied.getLocalPort());
      TaskWorkerManagerTwillRunnable runnable = initializedRunnable(cConf);
      try {
        try {
          runnable.run();
          Assert.fail("Expected RuntimeException when manager port is already bound");
        } catch (RuntimeException expected) {
          Assert.assertNotNull(expected.getCause());
        }
      } finally {
        runnable.destroy();
      }
    }
  }

  private TaskWorkerManagerTwillRunnable initializedRunnable(CConfiguration cConf)
      throws Exception {
    File cConfFile = tempFolder.newFile();
    File hConfFile = tempFolder.newFile();
    try (Writer writer = Files.newBufferedWriter(cConfFile.toPath(), StandardCharsets.UTF_8)) {
      cConf.writeXml(writer);
    }
    try (Writer writer = Files.newBufferedWriter(hConfFile.toPath(), StandardCharsets.UTF_8)) {
      new Configuration().writeXml(writer);
    }
    TaskWorkerManagerTwillRunnable runnable =
        new TaskWorkerManagerTwillRunnable(cConfFile.getAbsolutePath(), hConfFile.getAbsolutePath());
    TwillContext context = Mockito.mock(TwillContext.class);
    Mockito.when(context.getSpecification()).thenReturn(runnable.configure());
    runnable.initialize(context);
    return runnable;
  }
}
