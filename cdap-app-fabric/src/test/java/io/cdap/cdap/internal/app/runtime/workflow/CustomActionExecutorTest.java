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

import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.customaction.AbstractCustomAction;
import io.cdap.cdap.api.customaction.CustomActionSpecification;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.internal.app.runtime.ThrowingRunnable;
import io.cdap.cdap.internal.app.runtime.customaction.BasicCustomActionContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link CustomActionExecutor}.
 */
public class CustomActionExecutorTest {

  private static final AtomicBoolean ACTION_RAN = new AtomicBoolean(false);
  private static final AtomicBoolean ACTION_DESTROYED = new AtomicBoolean(false);

  /**
   * Test custom action that succeeds.
   */
  public static final class SucceedingAction extends AbstractCustomAction {

    @Override
    public void run() {
      ACTION_RAN.set(true);
    }

    @Override
    public void destroy() {
      ACTION_DESTROYED.set(true);
    }
  }

  /**
   * Test custom action that throws an unchecked exception in {@link #run()}.
   */
  public static final class UncheckedFailingAction extends AbstractCustomAction {

    @Override
    public void run() {
      throw new IllegalArgumentException("bad custom action input");
    }
  }

  @Test
  public void testExecuteSuccessTransitionsAndDestroys() throws Exception {
    ACTION_RAN.set(false);
    ACTION_DESTROYED.set(false);
    List<ProgramState> states = new ArrayList<>();
    BasicCustomActionContext context = createMockContext(SucceedingAction.class, states);

    CustomActionExecutor executor = new CustomActionExecutor(
        context, new InstantiatorFactory(false), CustomActionExecutorTest.class.getClassLoader());
    executor.execute();

    Assert.assertTrue(ACTION_RAN.get());
    Assert.assertTrue(ACTION_DESTROYED.get());
    Assert.assertEquals(3, states.size());
    Assert.assertEquals(ProgramStatus.INITIALIZING, states.get(0).getStatus());
    Assert.assertEquals(ProgramStatus.RUNNING, states.get(1).getStatus());
    Assert.assertEquals(ProgramStatus.COMPLETED, states.get(2).getStatus());
  }

  @Test
  public void testExecutePropagatesUncheckedAndCheckedExceptionsUnwrapped() throws Exception {
    List<ProgramState> states = new ArrayList<>();
    BasicCustomActionContext context = createMockContext(UncheckedFailingAction.class, states);
    CustomActionExecutor executor = new CustomActionExecutor(
        context, new InstantiatorFactory(false), CustomActionExecutorTest.class.getClassLoader());

    try {
      executor.execute();
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      Assert.assertEquals("bad custom action input", expected.getMessage());
    }
    Assert.assertEquals(ProgramStatus.FAILED, states.get(states.size() - 1).getStatus());
    Mockito.verify(context).destroyProgram(Mockito.any(), Mockito.any(), Mockito.eq(false));

    List<ProgramState> checkedStates = new ArrayList<>();
    BasicCustomActionContext checkedContext = createMockContext(SucceedingAction.class, checkedStates);
    IOException ioFailure = new IOException("io failure during initialize");
    Mockito.doThrow(ioFailure)
        .when(checkedContext)
        .initializeProgram(Mockito.any(), Mockito.any(), Mockito.eq(false));

    CustomActionExecutor checkedExecutor = new CustomActionExecutor(
        checkedContext, new InstantiatorFactory(false), CustomActionExecutorTest.class.getClassLoader());
    try {
      checkedExecutor.execute();
      Assert.fail("Expected IOException");
    } catch (IOException actual) {
      Assert.assertSame(ioFailure, actual);
    }
    Assert.assertEquals(ProgramStatus.FAILED, checkedStates.get(checkedStates.size() - 1).getStatus());
    Mockito.verify(checkedContext).destroyProgram(Mockito.any(), Mockito.any(), Mockito.eq(false));
  }

  @Test
  public void testExecutePropagatesErrorUnwrapped() throws Exception {
    List<ProgramState> states = new ArrayList<>();
    BasicCustomActionContext context = createMockContext(SucceedingAction.class, states);
    AssertionError fatalError = new AssertionError("fatal action error");
    Mockito.doThrow(fatalError).when(context).execute(Mockito.any(ThrowingRunnable.class));

    CustomActionExecutor executor = new CustomActionExecutor(
        context, new InstantiatorFactory(false), CustomActionExecutorTest.class.getClassLoader());
    try {
      executor.execute();
      Assert.fail("Expected AssertionError");
    } catch (AssertionError actual) {
      Assert.assertSame(fatalError, actual);
    }
    Assert.assertEquals(ProgramStatus.FAILED, states.get(states.size() - 1).getStatus());
    Mockito.verify(context).destroyProgram(Mockito.any(), Mockito.any(), Mockito.eq(false));
  }

  @SuppressWarnings("unchecked")
  private static BasicCustomActionContext createMockContext(
      Class<?> actionClass, List<ProgramState> recordedStates) throws Exception {
    CustomActionSpecification spec = Mockito.mock(CustomActionSpecification.class);
    Mockito.when(spec.getClassName()).thenReturn(actionClass.getName());
    Mockito.when(spec.getName()).thenReturn("testAction");
    Mockito.when(spec.getProperties()).thenReturn(Collections.emptyMap());
    Mockito.when(spec.getDatasets()).thenReturn(Collections.emptySet());
    BasicCustomActionContext context = Mockito.mock(BasicCustomActionContext.class);
    Mockito.when(context.getSpecification()).thenReturn(spec);
    Mockito.when(context.getMetrics()).thenReturn(Mockito.mock(Metrics.class));
    Mockito.when(context.getDefaultTxControl()).thenReturn(TransactionControl.IMPLICIT);
    Mockito.doAnswer(invocation -> {
      recordedStates.add(invocation.getArgument(0));
      return null;
    }).when(context).setState(Mockito.any(ProgramState.class));
    Mockito.doAnswer(invocation -> {
      ProgramLifecycle<BasicCustomActionContext> lifecycle = invocation.getArgument(0);
      lifecycle.initialize(context);
      return null;
    }).when(context).initializeProgram(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    Mockito.doAnswer(invocation -> {
      ThrowingRunnable runnable = invocation.getArgument(0);
      runnable.run();
      return null;
    }).when(context).execute(Mockito.any(ThrowingRunnable.class));
    Mockito.doAnswer(invocation -> {
      ProgramLifecycle<BasicCustomActionContext> lifecycle = invocation.getArgument(0);
      lifecycle.destroy();
      return null;
    }).when(context).destroyProgram(Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    return context;
  }
}
