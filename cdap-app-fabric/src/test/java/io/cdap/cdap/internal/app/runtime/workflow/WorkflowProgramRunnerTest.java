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

package io.cdap.cdap.internal.app.runtime.workflow;

import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.messaging.spi.MessagingService;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link WorkflowProgramRunner}.
 */
public class WorkflowProgramRunnerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testRunCompletesWorkflowAndClosesPluginInstantiator() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    WorkflowSpecification wfSpec = WorkflowDriverTest.createWorkflowSpec(Collections.emptyList());
    Program program = WorkflowDriverTest.createProgram(cConf, wfSpec, TEMP_FOLDER.newFolder());
    ProgramOptions options = WorkflowDriverTest.createProgramOptions(program.getId());

    File pluginsDir = TEMP_FOLDER.newFolder();
    AtomicBoolean closed = new AtomicBoolean(false);
    PluginInstantiator pluginInstantiator = new PluginInstantiator(
        cConf, program.getClassLoader(), pluginsDir) {
      @Override
      public void close() throws IOException {
        closed.set(true);
        super.close();
      }
    };

    WorkflowProgramRunner runner = createRunner(cConf, new NoOpMetricsCollectionService(), pluginInstantiator);
    ProgramController controller = runner.run(program, options);

    CountDownLatch completedLatch = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        if (currentState == ProgramController.State.COMPLETED) {
          completedLatch.countDown();
        }
      }

      @Override
      public void completed() {
        completedLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(completedLatch.await(5, TimeUnit.SECONDS));
    Assert.assertTrue(closed.get());
    program.close();
  }

  @Test
  public void testRunFailureClosesPluginInstantiatorAndPropagatesUncheckedUnwrapped() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    WorkflowSpecification wfSpec = WorkflowDriverTest.createWorkflowSpec(Collections.emptyList());
    Program program = WorkflowDriverTest.createProgram(cConf, wfSpec, TEMP_FOLDER.newFolder());
    ProgramOptions options = WorkflowDriverTest.createProgramOptions(program.getId());

    File pluginsDir = TEMP_FOLDER.newFolder();
    AtomicBoolean closed = new AtomicBoolean(false);
    PluginInstantiator pluginInstantiator = new PluginInstantiator(
        cConf, program.getClassLoader(), pluginsDir) {
      @Override
      public void close() throws IOException {
        closed.set(true);
        super.close();
        throw new IOException("ignored close failure");
      }
    };

    MetricsCollectionService failingMetrics = Mockito.mock(MetricsCollectionService.class);
    IllegalStateException expectedFailure = new IllegalStateException("workflow context init failure");
    Mockito.when(failingMetrics.getContext(Mockito.anyMap())).thenThrow(expectedFailure);

    WorkflowProgramRunner runner = createRunner(cConf, failingMetrics, pluginInstantiator);
    try {
      runner.run(program, options);
      Assert.fail("Expected IllegalStateException");
    } catch (IllegalStateException actual) {
      Assert.assertSame(expectedFailure, actual);
    }
    Assert.assertTrue(closed.get());
    program.close();
  }

  private static WorkflowProgramRunner createRunner(
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      @Nullable PluginInstantiator pluginInstantiator) {
    return new WorkflowProgramRunner(
        Mockito.mock(ProgramRunnerFactory.class),
        metricsCollectionService,
        Mockito.mock(DatasetFramework.class),
        Mockito.mock(DiscoveryServiceClient.class),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(WorkflowStateWriter.class),
        cConf,
        Mockito.mock(SecureStore.class),
        Mockito.mock(SecureStoreManager.class),
        Mockito.mock(MessagingService.class),
        Mockito.mock(ProgramStateWriter.class),
        Mockito.mock(MetadataReader.class),
        Mockito.mock(MetadataPublisher.class),
        Mockito.mock(FieldLineageWriter.class),
        Mockito.mock(NamespaceQueryAdmin.class),
        Mockito.mock(RemoteClientFactory.class),
        Mockito.mock(AppStateStoreProvider.class)) {
      @Nullable
      @Override
      protected PluginInstantiator createPluginInstantiator(ProgramOptions options, ClassLoader classLoader) {
        return pluginInstantiator;
      }
    };
  }
}
