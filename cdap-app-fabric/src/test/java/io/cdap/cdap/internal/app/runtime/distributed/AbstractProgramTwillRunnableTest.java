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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.runtime.AbstractProgramController;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.api.TwillContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractProgramTwillRunnable}.
 */
public class AbstractProgramTwillRunnableTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private String originalSecurityTtl;
  private String originalSystemTtl;

  /**
   * Closeable {@link ProgramRunner} interface for testing resource cleanup.
   */
  public interface CloseableProgramRunner extends ProgramRunner, Closeable {
  }

  /**
   * Concrete runnable subclass for testing {@link AbstractProgramTwillRunnable}.
   */
  public static final class TestProgramTwillRunnable
      extends AbstractProgramTwillRunnable<CloseableProgramRunner> {

    public TestProgramTwillRunnable() {
      super("test-runnable");
    }
  }

  @Before
  public void setUp() {
    originalSecurityTtl =
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL);
    originalSystemTtl = System.getProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);
  }

  @After
  public void tearDown() {
    Security.setProperty(
        AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL,
        originalSecurityTtl == null ? "" : originalSecurityTtl);
    if (originalSystemTtl == null) {
      System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);
    } else {
      System.setProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL, originalSystemTtl);
    }
  }

  @Test
  public void testDefaultsToThirtySecondsWhenUnset() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "");
    System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("30",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testDefaultsWhenSecurityPropertyIsBlank() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "  ");
    System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("30",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testPreservesPreSetSecurityProperty() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "60");
    System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("60",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testPreservesPreSetSystemProperty() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "");
    System.setProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL, "45");

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testPreservesExplicitForeverPolicy() {
    Security.setProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL, "-1");
    System.clearProperty(AbstractProgramTwillRunnable.SUN_NET_INETADDR_TTL);

    AbstractProgramTwillRunnable.initializeDnsCachePolicy();
    Assert.assertEquals("-1",
        Security.getProperty(AbstractProgramTwillRunnable.NETWORKADDRESS_CACHE_TTL));
  }

  @Test
  public void testInitializePropagatesUncheckedAndWrapsChecked() throws Exception {
    TestProgramTwillRunnable runnable = new TestProgramTwillRunnable();
    TwillContext uncheckedContext = Mockito.mock(TwillContext.class);
    IllegalStateException unchecked = new IllegalStateException("context args failed");
    Mockito.when(uncheckedContext.getApplicationArguments()).thenThrow(unchecked);

    try {
      runnable.initialize(uncheckedContext);
      Assert.fail("Expected IllegalStateException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(unchecked, thrown);
    }

    File missingOptionsFile = new File(tempFolder.getRoot(), "missing-options.json");
    TwillContext ioContext = Mockito.mock(TwillContext.class);
    Mockito.when(ioContext.getApplicationArguments())
        .thenReturn(new String[]{missingOptionsFile.getAbsolutePath()});

    try {
      runnable.initialize(ioContext);
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException thrown) {
      Assert.assertTrue(thrown.getCause() instanceof IOException);
    }
  }

  @Test
  public void testRunStartsAndStopsServicesInReverseAndClosesQuietlyOnIoFailure() throws Exception {
    List<String> events = new ArrayList<>();
    final Service service1 = createTrackingService("s1", events, false);
    final Service service2 = createTrackingService("s2", events, true);

    ProgramId programId = NamespaceId.DEFAULT.app("app").worker("wrk");
    final ProgramRunId runId = programId.run(RunIds.generate());
    FakeController controller = new FakeController(runId);
    controller.completeImmediately();

    AtomicBoolean programClosed = new AtomicBoolean(false);
    Program program = Mockito.mock(Program.class);
    Mockito.doAnswer(inv -> {
      programClosed.set(true);
      throw new IOException("program close io failure");
    }).when(program).close();

    AtomicBoolean runnerClosed = new AtomicBoolean(false);
    CloseableProgramRunner runner = Mockito.mock(CloseableProgramRunner.class);
    Mockito.when(runner.run(Mockito.eq(program), Mockito.any(ProgramOptions.class)))
        .thenReturn(controller);
    Mockito.doAnswer(inv -> {
      runnerClosed.set(true);
      throw new IOException("runner close io failure");
    }).when(runner).close();

    AtomicBoolean logInitializerClosed = new AtomicBoolean(false);
    LogAppenderInitializer logInitializer = new LogAppenderInitializer(null) {
      @Override
      public void close() {
        logInitializerClosed.set(true);
        sneakyThrow(new IOException("log close io failure"));
      }
    };

    TestProgramTwillRunnable runnable = createWiredRunnable(
        runId, program, runner, logInitializer, service1, service2);
    runnable.run();

    Assert.assertTrue(programClosed.get());
    Assert.assertTrue(runnerClosed.get());
    Assert.assertTrue(logInitializerClosed.get());
    Assert.assertEquals(
        Arrays.asList("start:s1", "start:s2", "stop:s2", "stop:s1"), events);
    Assert.assertEquals(Service.State.TERMINATED, service1.state());
  }

  @Test
  public void testRunPropagatesRootCauseOnExecutionFailureAndHandlesNullCloseables()
      throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("app").worker("wrk");
    final ProgramRunId runId = programId.run(RunIds.generate());

    IllegalArgumentException uncheckedRoot = new IllegalArgumentException("root failure");
    FakeController uncheckedFailController = new FakeController(runId);
    uncheckedFailController.failImmediately(new Exception("outer wrapper", uncheckedRoot));

    CloseableProgramRunner runner = Mockito.mock(CloseableProgramRunner.class);
    Mockito.when(runner.run(Mockito.any(), Mockito.any())).thenReturn(uncheckedFailController);

    // Also pass null program and null logAppenderInitializer to verify null-safe closeQuietly
    Service coreService = createTrackingService("core", new ArrayList<>(), false);
    TestProgramTwillRunnable runnable =
        createWiredRunnable(runId, null, runner, null, coreService);

    try {
      runnable.run();
      Assert.fail("Expected IllegalArgumentException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(uncheckedRoot, thrown);
    }
    Assert.assertEquals(Service.State.TERMINATED, coreService.state());

    // Checked root cause must be wrapped in RuntimeException
    IOException checkedRoot = new IOException("checked root failure");
    FakeController checkedFailController = new FakeController(runId);
    checkedFailController.failImmediately(checkedRoot);
    Mockito.when(runner.run(Mockito.any(), Mockito.any())).thenReturn(checkedFailController);
    Service coreService2 = createTrackingService("core2", new ArrayList<>(), false);
    TestProgramTwillRunnable checkedRunnable =
        createWiredRunnable(runId, null, runner, null, coreService2);

    try {
      checkedRunnable.run();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException thrown) {
      Assert.assertSame(checkedRoot, thrown.getCause());
    }
    Assert.assertEquals(Service.State.TERMINATED, coreService2.state());
  }

  @Test
  public void testStopPropagatesExecutionExceptionFromControllerStop() throws Exception {
    ProgramId programId = NamespaceId.DEFAULT.app("app").worker("wrk");
    final ProgramRunId runId = programId.run(RunIds.generate());

    TestProgramTwillRunnable runnable = new TestProgramTwillRunnable();
    // Calling stop before initialization (programCompletion == null) is a safe no-op
    runnable.stop();

    TwillContext twillContext = Mockito.mock(TwillContext.class);
    Mockito.when(twillContext.getTerminationTimeoutMillis()).thenReturn(2000L);

    ProgramController failingStopController = Mockito.mock(ProgramController.class);
    IOException stopFailure = new IOException("stop failed");
    Mockito.when(failingStopController.stop(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS)))
        .thenReturn(Futures.immediateFailedFuture(stopFailure));

    CompletableFuture<ProgramController> controllerFuture =
        CompletableFuture.completedFuture(failingStopController);
    final CompletableFuture<ProgramController.State> programCompletion = new CompletableFuture<>();

    setField(runnable, "programRunId", runId);
    setField(runnable, "context", twillContext);
    setField(runnable, "controllerFuture", controllerFuture);
    setField(runnable, "programCompletion", programCompletion);

    try {
      runnable.stop();
      Assert.fail("Expected RuntimeException wrapping ExecutionException");
    } catch (RuntimeException thrown) {
      Assert.assertTrue(thrown.getCause() instanceof ExecutionException);
      Assert.assertSame(stopFailure, thrown.getCause().getCause());
    }
  }

  private TestProgramTwillRunnable createWiredRunnable(
      ProgramRunId runId,
      Program program,
      CloseableProgramRunner runner,
      LogAppenderInitializer logInitializer,
      Service... services) throws Exception {
    TestProgramTwillRunnable runnable = new TestProgramTwillRunnable();
    Deque<Service> deque = new LinkedList<>();
    for (Service service : services) {
      deque.add(service);
    }
    ProgramOptions options = new SimpleProgramOptions(
        runId.getParent(), new BasicArguments(), new BasicArguments());
    setField(runnable, "programRunId", runId);
    setField(runnable, "programOptions", options);
    setField(runnable, "program", program);
    setField(runnable, "programRunner", runner);
    setField(runnable, "logAppenderInitializer", logInitializer);
    setField(runnable, "coreServices", deque);
    setField(runnable, "controllerFuture", new CompletableFuture<ProgramController>());
    setField(runnable, "programCompletion", new CompletableFuture<ProgramController.State>());
    return runnable;
  }

  private Service createTrackingService(
      String label, List<String> events, boolean failOnStop) {
    return new AbstractIdleService() {
      @Override
      protected void startUp() {
        events.add("start:" + label);
      }

      @Override
      protected void shutDown() {
        events.add("stop:" + label);
        if (failOnStop) {
          throw new IllegalStateException("stop failed for " + label);
        }
      }
    };
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = AbstractProgramTwillRunnable.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
    throw (E) e;
  }

  private static final class FakeController extends AbstractProgramController {

    private FakeController(ProgramRunId runId) {
      super(runId);
    }

    private void completeImmediately() {
      started();
      complete();
    }

    private void failImmediately(Throwable cause) {
      started();
      error(cause);
    }

    @Override
    protected void doSuspend() {
      // no-op
    }

    @Override
    protected void doResume() {
      // no-op
    }

    @Override
    protected void doStop() {
      // no-op
    }

    @Override
    protected void doCommand(String name, Object value) {
      // no-op
    }
  }
}
