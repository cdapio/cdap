/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import io.cdap.cdap.app.preview.DefaultPreviewRunnerManager;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewRunnerManager;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.k8s.PreviewRequestPollerInfo;
import io.cdap.cdap.proto.id.ApplicationId;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillContext;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit test for {@link PreviewRunnerTwillRunnable}.
 */
public class PreviewRunnerTwillRunnableTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testNoSQLInjector() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);
    Injector injector = PreviewRunnerTwillRunnable.createInjector(cConf, new Configuration(),
                                                                  new PreviewRequestPollerInfo(0, "testuid", null));
    DefaultPreviewRunnerManager defaultPreviewRunnerManager = (DefaultPreviewRunnerManager) injector
      .getInstance(PreviewRunnerManager.class);
    Injector previewInjector = defaultPreviewRunnerManager.createPreviewInjector();
    previewInjector.getInstance(PreviewRunner.class);
  }

  @Test
  public void testPostgresQLInjector() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_SQL);
    Injector injector = PreviewRunnerTwillRunnable.createInjector(cConf, new Configuration(),
                                                                  new PreviewRequestPollerInfo(0, "testuid", null));
    DefaultPreviewRunnerManager defaultPreviewRunnerManager = (DefaultPreviewRunnerManager) injector
      .getInstance(PreviewRunnerManager.class);
    Injector previewInjector = defaultPreviewRunnerManager.createPreviewInjector();
    previewInjector.getInstance(PreviewRunner.class);
  }

  @Test
  public void testInitializePropagatesRuntimeExceptionUnwrapped() throws Exception {
    File hConfFile = TEMP_FOLDER.newFile("hconf.xml");
    try (FileOutputStream fos = new FileOutputStream(hConfFile)) {
      new Configuration().writeXml(fos);
    }
    File cConfFile = TEMP_FOLDER.newFile("cconf.xml");
    try (FileOutputStream fos = new FileOutputStream(cConfFile)) {
      CConfiguration.create().writeXml(fos);
    }

    PreviewRunnerTwillRunnable runnable =
        new PreviewRunnerTwillRunnable(cConfFile.getAbsolutePath(), hConfFile.getAbsolutePath());
    TwillContext context = Mockito.mock(TwillContext.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(context.getSpecification().getConfigs()).thenReturn(
        ImmutableMap.of("cConf", cConfFile.getAbsolutePath(), "hConf", hConfFile.getAbsolutePath()));
    Mockito.when(context.getArguments()).thenReturn(new String[0]);
    Mockito.when(context.getApplicationArguments()).thenReturn(new String[0]);
    IllegalStateException expected = new IllegalStateException("twill context failure");
    Mockito.when(context.getInstanceId()).thenThrow(expected);

    try {
      runnable.initialize(context);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual);
    }

    IOException checkedIo = new IOException("checked twill failure");
    Mockito.doAnswer(invocation -> {
      throw checkedIo;
    }).when(context).getInstanceId();
    RuntimeException wrapped =
        Assert.assertThrows(RuntimeException.class, () -> runnable.initialize(context));
    Assert.assertSame(checkedIo, wrapped.getCause());
  }

  @Test
  public void testRunAndStopWithPreviewRunnerManager() throws Exception {
    PreviewRunnerTwillRunnable runnable =
        new PreviewRunnerTwillRunnable("cconf.xml", "hconf.xml");
    FakePreviewRunnerManager manager = new FakePreviewRunnerManager(false);
    Field managerField = PreviewRunnerTwillRunnable.class.getDeclaredField("previewRunnerManager");
    managerField.setAccessible(true);
    managerField.set(runnable, manager);

    CountDownLatch runFinished = new CountDownLatch(1);
    Thread runnerThread = new Thread(() -> {
      runnable.run();
      runFinished.countDown();
    });
    runnerThread.start();

    Tasks.waitFor(Service.State.RUNNING, manager::state, 5, TimeUnit.SECONDS,
        20, TimeUnit.MILLISECONDS);
    runnable.stop();
    Assert.assertTrue(runFinished.await(5, TimeUnit.SECONDS));
    Assert.assertEquals(Service.State.TERMINATED, manager.state());
  }

  @Test
  public void testRunHandlesPreviewRunnerManagerFailure() throws Exception {
    PreviewRunnerTwillRunnable runnable =
        new PreviewRunnerTwillRunnable("cconf.xml", "hconf.xml");
    FakePreviewRunnerManager failingManager = new FakePreviewRunnerManager(true);
    Field managerField = PreviewRunnerTwillRunnable.class.getDeclaredField("previewRunnerManager");
    managerField.setAccessible(true);
    managerField.set(runnable, failingManager);

    runnable.run();
    Assert.assertEquals(Service.State.FAILED, failingManager.state());
  }

  private static final class FakePreviewRunnerManager extends AbstractIdleService
      implements PreviewRunnerManager {

    private final boolean failOnStart;

    private FakePreviewRunnerManager(boolean failOnStart) {
      this.failOnStart = failOnStart;
    }

    @Override
    protected void startUp() {
      if (failOnStart) {
        throw new IllegalStateException("manager startup failure");
      }
    }

    @Override
    protected void shutDown() {
      // no-op
    }

    @Override
    public void stop(ApplicationId preview) {
      // no-op
    }

    @Override
    public void stop(byte[] pollerInfo) {
      // no-op
    }
  }
}
