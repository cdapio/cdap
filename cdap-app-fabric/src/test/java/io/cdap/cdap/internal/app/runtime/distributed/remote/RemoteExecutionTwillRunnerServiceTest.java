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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import io.cdap.cdap.internal.app.runtime.distributed.RunnableDefinition;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RemoteExecutionTwillRunnerService} and {@link RemoteExecutionService}.
 */
public class RemoteExecutionTwillRunnerServiceTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private CConfiguration cConf;
  private ProvisioningService provisioningService;
  private ProgramStateWriter programStateWriter;
  private RemoteExecutionTwillRunnerService runnerService;

  @Before
  public void setUp() throws IOException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempFolder.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    provisioningService = Mockito.mock(ProvisioningService.class);
    programStateWriter = Mockito.mock(ProgramStateWriter.class);
  }

  @After
  public void tearDown() {
    if (runnerService != null) {
      runnerService.stop();
    }
  }

  @Test
  public void testPrepareRejectsSshMonitorType() throws IOException {
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    RemoteExecutionTwillRunnerService service = createAndInitRunner(cConf);
    ProgramRunId runId = NamespaceId.DEFAULT.app("a").workflow("wf").run(RunIds.generate());
    ProgramOptions opts = new SimpleProgramOptions(
        runId.getParent(),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc")),
        new BasicArguments(Collections.singletonMap(
            SystemArguments.RUNTIME_MONITOR_TYPE, "ssh")));
    ProgramTwillApplication app = createProgramApp(runId, opts);

    UnsupportedOperationException e = Assert.assertThrows(UnsupportedOperationException.class,
        () -> service.prepare(app));
    Assert.assertTrue(e.getMessage().contains("Only URL runtime monitoring is supported"));
  }

  @Test
  public void testPrepareRejectsMissingAndWhitespaceMonitorUrl() throws IOException {
    ProgramRunId runId = NamespaceId.DEFAULT.app("a").workflow("wf").run(RunIds.generate());
    ProgramOptions opts = new SimpleProgramOptions(
        runId.getParent(), new BasicArguments(), new BasicArguments());
    ProgramTwillApplication app = createProgramApp(runId, opts);

    for (String url : new String[]{null, "", "   "}) {
      CConfiguration confCopy = CConfiguration.copy(cConf);
      if (url != null) {
        confCopy.set(Constants.RuntimeMonitor.MONITOR_URL, url);
      }
      RemoteExecutionTwillRunnerService service = createAndInitRunner(confCopy);
      IllegalStateException e = Assert.assertThrows(IllegalStateException.class,
          () -> service.prepare(app));
      Assert.assertTrue(e.getMessage().contains(Constants.RuntimeMonitor.MONITOR_URL));
      service.stop();
      this.runnerService = null;
    }
  }

  @Test
  public void testPrepareSucceedsForUrlWithoutSecureKeysDirWhenInternalAuthDisabled()
      throws IOException {
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    RemoteExecutionTwillRunnerService service = createAndInitRunner(cConf);
    ProgramRunId runId = NamespaceId.DEFAULT.app("a").workflow("wf").run(RunIds.generate());
    ProgramOptions opts = new SimpleProgramOptions(
        runId.getParent(), new BasicArguments(), new BasicArguments());

    TwillPreparer preparer = service.prepare(createProgramApp(runId, opts));
    Assert.assertTrue(preparer instanceof RuntimeJobTwillPreparer);
  }

  @Test
  public void testCreateControllerFromRunRecordRejectsSshAndMissingMonitorUrl()
      throws IOException {
    ProgramRunId runId = NamespaceId.DEFAULT.app("a").workflow("wf").run(RunIds.generate());

    // Missing MONITOR_URL
    RemoteExecutionTwillRunnerService missingUrlService = createAndInitRunner(cConf);
    RunRecordDetail urlRecord = createIsolatedRunRecord(
        runId, Collections.emptyMap(), Collections.emptyMap());
    IllegalStateException missingErr = Assert.assertThrows(IllegalStateException.class,
        () -> missingUrlService.createTwillControllerFromRunRecord(urlRecord));
    Assert.assertTrue(missingErr.getMessage().contains(Constants.RuntimeMonitor.MONITOR_URL));
    missingUrlService.stop();
    this.runnerService = null;

    // Explicit SSH monitor type
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    RemoteExecutionTwillRunnerService sshService = createAndInitRunner(cConf);
    RunRecordDetail sshRecord = createIsolatedRunRecord(
        runId,
        Collections.emptyMap(),
        Collections.singletonMap(SystemArguments.RUNTIME_MONITOR_TYPE, "ssh"));
    UnsupportedOperationException sshErr = Assert.assertThrows(UnsupportedOperationException.class,
        () -> sshService.createTwillControllerFromRunRecord(sshRecord));
    Assert.assertTrue(sshErr.getMessage().contains("Only URL runtime monitoring is supported"));
  }

  @Test
  public void testMissingRuntimeJobManagerThrowsUnsupportedAndAbortsRetriesImmediately()
      throws Exception {
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    Mockito.when(provisioningService.getRuntimeJobManager(Mockito.any(), Mockito.any()))
        .thenReturn(Optional.empty());

    ProgramRunId runId = NamespaceId.DEFAULT.app("a").workflow("wf").run(RunIds.generate());
    RemoteExecutionTwillRunnerService service = createAndInitRunner(cConf);
    RunRecordDetail record = createIsolatedRunRecord(
        runId,
        Collections.singletonMap(SystemArguments.PROFILE_PROVISIONER, "legacy-ssh"),
        Collections.emptyMap());

    // End-to-end verification: createTwillControllerFromRunRecord wires
    // getRuntimeJobManagerSupplier into RuntimeJobRemoteProcessController and
    // RemoteExecutionService, which must fail fast without retrying on
    // UnsupportedOperationException and publish the program error.
    TwillController controller = service.createTwillControllerFromRunRecord(record);
    ExecutionException execErr = Assert.assertThrows(ExecutionException.class,
        () -> controller.awaitTerminated(5, TimeUnit.SECONDS));
    Assert.assertTrue("Expected UnsupportedOperationException cause, got: " + execErr.getCause(),
        execErr.getCause() instanceof UnsupportedOperationException);
    Assert.assertTrue(execErr.getCause().getMessage().contains("legacy-ssh"));
    Assert.assertTrue(execErr.getCause().getMessage().contains(runId.toString()));

    ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
    Mockito.verify(programStateWriter).error(Mockito.eq(runId), errorCaptor.capture());
    Assert.assertTrue(errorCaptor.getValue() instanceof UnsupportedOperationException);
    Assert.assertTrue(errorCaptor.getValue().getMessage().contains(runId.toString()));

    // Verify controller.complete() also returns immediately instead of retrying for stopDelayMillis.
    long startNanos = System.nanoTime();
    ((RemoteExecutionTwillController) controller).complete();
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    Assert.assertTrue("Expected complete() to abort immediately without retrying, took "
        + elapsedMillis + "ms", elapsedMillis < 2000);
  }

  private RemoteExecutionTwillRunnerService createAndInitRunner(CConfiguration conf)
      throws IOException {
    RemoteExecutionTwillRunnerService service = new RemoteExecutionTwillRunnerService(
        conf, new Configuration(), new LocalLocationFactory(tempFolder.newFolder()),
        provisioningService, programStateWriter, null, null, null);
    service.doInitialize();
    this.runnerService = service;
    return service;
  }

  private ProgramTwillApplication createProgramApp(ProgramRunId runId, ProgramOptions opts) {
    RunnableDefinition runnableDef = new RunnableDefinition(
        new DummyTwillRunnable(), ResourceSpecification.BASIC,
        Collections.emptyMap(), Collections.emptyMap(), null);
    return new ProgramTwillApplication(
        runId, opts, Collections.singletonMap("dummy", runnableDef),
        Collections.emptyList(), Collections.emptyMap(), null);
  }

  private RunRecordDetail createIsolatedRunRecord(ProgramRunId runId,
      Map<String, String> systemArgs, Map<String, String> userArgs) {
    Map<String, String> allSysArgs = new HashMap<>(systemArgs);
    allSysArgs.put(ProgramOptionConstants.CLUSTER_MODE, ClusterMode.ISOLATED.name());
    RunRecordDetail record = Mockito.mock(RunRecordDetail.class);
    Mockito.when(record.getProgramRunId()).thenReturn(runId);
    Mockito.when(record.getSystemArgs()).thenReturn(allSysArgs);
    Mockito.when(record.getUserArgs()).thenReturn(userArgs);
    return record;
  }

  /**
   * Minimal {@link AbstractTwillRunnable} for testing {@link ProgramTwillApplication}.
   */
  public static final class DummyTwillRunnable extends AbstractTwillRunnable {

    @Override
    public void run() {
      // no-op
    }
  }
}
