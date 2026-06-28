package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobStatus;
import org.apache.twill.api.TwillController;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RemoteExecutionTwillController}.
 */
public class RemoteExecutionTwillControllerTest {

  private CConfiguration cConf;
  private ProgramRunId programRunId;
  private RemoteProcessController remoteProcessController;
  private ScheduledExecutorService scheduler;
  private RemoteExecutionService remoteExecutionService;
  private CompletableFuture<Void> startupCompletionFuture;

  @Before
  public void setUp() {
    cConf = CConfiguration.create();
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 100);
    programRunId = new ProgramRunId(NamespaceId.DEFAULT.getNamespace(), "testapp", ProgramType.WORKFLOW, "testworkflow", "testrun");
    remoteProcessController = Mockito.mock(RemoteProcessController.class);
    scheduler = Executors.newSingleThreadScheduledExecutor();
    remoteExecutionService = Mockito.mock(RemoteExecutionService.class);
    startupCompletionFuture = new CompletableFuture<>();
  }

  private RemoteExecutionTwillController createController(boolean terminateWithController) {
    return new RemoteExecutionTwillController(cConf, programRunId, startupCompletionFuture,
                                            remoteProcessController, scheduler, remoteExecutionService,
                                            terminateWithController);
  }

  @Test
  public void testComplete_KillSkippedForTerminalState() throws Exception {
    RemoteExecutionTwillController controller = createController(true);
    startupCompletionFuture.complete(null);

    // Simulate getStatus throwing an exception to enter the catch block
    when(remoteProcessController.getStatus()).thenThrow(new RuntimeException("Simulated poll failure"));

    // In the catch block, simulate the job being COMPLETED
    when(remoteProcessController.getStatus()).thenReturn(RuntimeJobStatus.COMPLETED);

    controller.complete();

    // Verify kill was NOT called because the status was terminal
    verify(remoteProcessController, never()).kill(any());
  }

  @Test
  public void testComplete_KillCalledForRunningState() throws Exception {
    RemoteExecutionTwillController controller = createController(true);
    startupCompletionFuture.complete(null);

    // Simulate getStatus throwing an exception to enter the catch block
    when(remoteProcessController.getStatus()).thenThrow(new RuntimeException("Simulated poll failure"));
    // In the catch block, simulate the job being RUNNING
    when(remoteProcessController.getStatus()).thenReturn(RuntimeJobStatus.RUNNING);

    controller.complete();

    // Verify kill WAS called
    verify(remoteProcessController).kill(RuntimeJobStatus.RUNNING);
  }

  @Test
  public void testComplete_StatusCheckFailsInCatch() throws Exception {
    RemoteExecutionTwillController controller = createController(true);
    startupCompletionFuture.complete(null);

    // Simulate getStatus throwing an exception to enter the catch block
    when(remoteProcessController.getStatus()).thenThrow(new RuntimeException("Simulated poll failure"))
                                       .thenThrow(new RuntimeException("Simulated getStatus failure in catch"));

    controller.complete();

    // Verify kill was NOT called because getStatus failed in the catch
    verify(remoteProcessController, never()).kill(any());
  }
}
