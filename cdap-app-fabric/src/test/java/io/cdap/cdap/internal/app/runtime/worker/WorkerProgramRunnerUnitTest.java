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

package io.cdap.cdap.internal.app.runtime.worker;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.internal.app.runtime.AppStateStoreProvider;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Fast unit tests for {@link WorkerProgramRunner} resource cleanup and validation paths.
 */
public class WorkerProgramRunnerUnitTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStartupFailureClosesPluginInstantiatorQuietlyAndRethrows() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File pluginsDir = TEMP_FOLDER.newFolder();
    File unpackedDir = TEMP_FOLDER.newFolder();
    ProgramClassLoader programClassLoader = new ProgramClassLoader(
        cConf, unpackedDir, WorkerProgramRunnerUnitTest.class.getClassLoader());
    AtomicBoolean instantiatorClosed = new AtomicBoolean(false);

    PluginInstantiator trackingInstantiator = new PluginInstantiator(
        cConf, programClassLoader, pluginsDir) {
      @Override
      public void close() throws IOException {
        instantiatorClosed.set(true);
        super.close();
        throw new IOException("simulated close failure");
      }
    };

    MetricsCollectionService failingMetrics = Mockito.mock(MetricsCollectionService.class);
    IllegalStateException expectedFailure = new IllegalStateException("metrics init failed");
    Mockito.when(failingMetrics.getContext(Mockito.anyMap())).thenThrow(expectedFailure);

    WorkerProgramRunner runner = createRunner(cConf, failingMetrics, trackingInstantiator);
    Program program = createMockWorkerProgram(programClassLoader);
    ProgramOptions options = createValidWorkerOptions(program.getId());

    try {
      runner.run(program, options);
      Assert.fail("Expected IllegalStateException from WorkerProgramRunner.run");
    } catch (IllegalStateException actual) {
      Assert.assertSame(expectedFailure, actual);
    }
    Assert.assertTrue(instantiatorClosed.get());
    programClassLoader.close();
  }

  @Test
  public void testStartupFailureWithNullPluginInstantiatorRethrowsOriginalThrowable() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File unpackedDir = TEMP_FOLDER.newFolder();
    ProgramClassLoader programClassLoader = new ProgramClassLoader(
        cConf, unpackedDir, WorkerProgramRunnerUnitTest.class.getClassLoader());
    MetricsCollectionService failingMetrics = Mockito.mock(MetricsCollectionService.class);
    IllegalStateException expectedFailure =
        new IllegalStateException("metrics init failed with null plugin");
    Mockito.when(failingMetrics.getContext(Mockito.anyMap())).thenThrow(expectedFailure);

    WorkerProgramRunner runner = createRunner(cConf, failingMetrics, null);
    Program program = createMockWorkerProgram(programClassLoader);
    ProgramOptions options = createValidWorkerOptions(program.getId());

    try {
      runner.run(program, options);
      Assert.fail("Expected IllegalStateException from WorkerProgramRunner.run");
    } catch (IllegalStateException actual) {
      Assert.assertSame(expectedFailure, actual);
    }
    programClassLoader.close();
  }

  private static WorkerProgramRunner createRunner(
      CConfiguration cConf,
      MetricsCollectionService metricsCollectionService,
      @Nullable PluginInstantiator pluginInstantiator) {
    return new WorkerProgramRunner(
        cConf,
        metricsCollectionService,
        Mockito.mock(DatasetFramework.class),
        Mockito.mock(DiscoveryServiceClient.class),
        Mockito.mock(TransactionSystemClient.class),
        Mockito.mock(SecureStore.class),
        Mockito.mock(SecureStoreManager.class),
        Mockito.mock(MessagingService.class),
        Mockito.mock(MetadataReader.class),
        Mockito.mock(MetadataPublisher.class),
        Mockito.mock(NamespaceQueryAdmin.class),
        Mockito.mock(FieldLineageWriter.class),
        Mockito.mock(RemoteClientFactory.class),
        Mockito.mock(AppStateStoreProvider.class)) {
      @Nullable
      @Override
      protected PluginInstantiator createPluginInstantiator(ProgramOptions options, ClassLoader classLoader) {
        return pluginInstantiator;
      }
    };
  }

  private static Program createMockWorkerProgram(ClassLoader classLoader) {
    ProgramId programId = NamespaceId.DEFAULT.app("workerApp").worker("myWorker");
    WorkerSpecification workerSpec = new WorkerSpecification(
        "io.cdap.cdap.DummyWorker", "myWorker", "desc",
        Collections.emptyMap(), Collections.emptySet(), new Resources(), 1, Collections.emptyMap());
    ApplicationSpecification appSpec = Mockito.mock(ApplicationSpecification.class);
    Mockito.when(appSpec.getName()).thenReturn("workerApp");
    Mockito.when(appSpec.getWorkers()).thenReturn(ImmutableMap.of("myWorker", workerSpec));
    Mockito.when(appSpec.getPlugins()).thenReturn(Collections.emptyMap());

    Program program = Mockito.mock(Program.class);
    Mockito.when(program.getId()).thenReturn(programId);
    Mockito.when(program.getName()).thenReturn("myWorker");
    Mockito.when(program.getType()).thenReturn(ProgramType.WORKER);
    Mockito.when(program.getApplicationSpecification()).thenReturn(appSpec);
    Mockito.when(program.getClassLoader()).thenReturn(classLoader);
    return program;
  }

  private static ProgramOptions createValidWorkerOptions(ProgramId programId) {
    Map<String, String> systemArgs = ImmutableMap.of(
        ProgramOptionConstants.INSTANCE_ID, "0",
        ProgramOptionConstants.INSTANCES, "1",
        ProgramOptionConstants.RUN_ID, RunIds.generate().getId(),
        ProgramOptionConstants.ARTIFACT_ID, "default:art:1.0.0");
    return new SimpleProgramOptions(programId, new BasicArguments(systemArgs), new BasicArguments());
  }
}
