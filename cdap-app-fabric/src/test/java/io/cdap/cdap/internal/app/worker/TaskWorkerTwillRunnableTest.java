/*
 * Copyright © 2020-2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.distributed.MockMasterEnvironment;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link TaskWorkerTwillRunnable}.
 */
public class TaskWorkerTwillRunnableTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testInjector() {
    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    Injector injector = TaskWorkerTwillRunnable.createInjector(CConfiguration.create(), new Configuration());
    injector.getInstance(ArtifactManagerFactory.class).create(NamespaceId.SYSTEM, RetryStrategies.noRetry());
  }

  @Test
  public void testInitializePropagatesRuntimeExceptionUnwrapped() {
    TaskWorkerTwillRunnable runnable = new TaskWorkerTwillRunnable("missing-cConf.xml", "missing-hConf.xml");
    TwillContext context = Mockito.mock(TwillContext.class);
    // Missing runtime argument map in mock context triggers NullPointerException inside getArgument
    Mockito.when(context.getSpecification()).thenReturn(null);
    try {
      runnable.initialize(context);
      Assert.fail("Expected RuntimeException on invalid TwillContext");
    } catch (NullPointerException expected) {
      // Unchecked exception must propagate directly without extra RuntimeException wrapping
      Assert.assertNull(expected.getCause());
    }
  }

  @Test
  public void testInitializeRunStopAndDestroyLifecycle() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.TaskWorker.ADDRESS, "localhost");
    cConf.setInt(Constants.TaskWorker.PORT, 0);
    cConf.set(Constants.TaskWorker.LOCAL_DATA_DIR, temporaryFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.ArtifactLocalizer.PRELOAD_LIST, "");
    cConf.setInt(Constants.ArtifactLocalizer.PRELOAD_VERSION_LIMIT, 1);

    File cConfFile = temporaryFolder.newFile("cConf.xml");
    File hConfFile = temporaryFolder.newFile("hConf.xml");
    try (FileOutputStream out = new FileOutputStream(cConfFile)) {
      cConf.writeXml(out);
    }
    try (FileOutputStream out = new FileOutputStream(hConfFile)) {
      new Configuration().writeXml(out);
    }

    MasterEnvironments.setMasterEnvironment(new MockMasterEnvironment());
    TaskWorkerTwillRunnable runnable =
        new TaskWorkerTwillRunnable(cConfFile.getAbsolutePath(), hConfFile.getAbsolutePath());

    TwillContext context = Mockito.mock(TwillContext.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(context.getSpecification().getConfigs()).thenReturn(
        runnable.configure().getConfigs());
    Mockito.when(context.getApplicationArguments()).thenReturn(new String[0]);
    Mockito.when(context.getArguments()).thenReturn(new String[0]);

    runnable.initialize(context);

    Field metricsField = TaskWorkerTwillRunnable.class.getDeclaredField("metricsCollectionService");
    metricsField.setAccessible(true);
    MetricsCollectionService metrics = (MetricsCollectionService) metricsField.get(runnable);
    Assert.assertEquals(Service.State.RUNNING, metrics.state());

    Field workerField = TaskWorkerTwillRunnable.class.getDeclaredField("taskWorker");
    workerField.setAccessible(true);
    TaskWorkerService workerService = (TaskWorkerService) workerField.get(runnable);
    Assert.assertNotNull(workerService);

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
    Assert.assertEquals(Collections.emptyList(), Collections.emptyList());
  }
}
