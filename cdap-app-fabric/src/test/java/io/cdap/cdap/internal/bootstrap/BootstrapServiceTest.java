/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.internal.bootstrap;

import com.google.gson.JsonObject;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.bootstrap.executor.BaseStepExecutor;
import io.cdap.cdap.internal.bootstrap.executor.BootstrapStepExecutor;
import io.cdap.cdap.internal.bootstrap.executor.EmptyArguments;
import io.cdap.cdap.internal.capability.CapabilityManagementService;
import io.cdap.cdap.internal.sysapp.SystemAppManagementService;
import io.cdap.cdap.proto.bootstrap.BootstrapResult;
import io.cdap.cdap.proto.bootstrap.BootstrapStepResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the {@link BootstrapService}.
 */
public class BootstrapServiceTest {
  private static final BootstrapStep STEP1 = new BootstrapStep("s1", BootstrapStep.Type.CREATE_NATIVE_PROFILE,
                                                               BootstrapStep.RunCondition.ONCE, new JsonObject());
  private static final BootstrapStep STEP2 = new BootstrapStep("s2", BootstrapStep.Type.LOAD_SYSTEM_ARTIFACTS,
                                                               BootstrapStep.RunCondition.ALWAYS, new JsonObject());
  private static final BootstrapStep STEP3 = new BootstrapStep("s3", BootstrapStep.Type.CREATE_DEFAULT_NAMESPACE,
                                                               BootstrapStep.RunCondition.ONCE, new JsonObject());
  private static final FakeExecutor EXECUTOR1 = new FakeExecutor();
  private static final FakeExecutor EXECUTOR2 = new FakeExecutor();
  private static final FakeExecutor EXECUTOR3 = new FakeExecutor();

  private static BootstrapConfig bootstrapConfig;
  private static BootstrapService bootstrapService;
  private static BootstrapStore bootstrapStore;
  private static SystemAppManagementService systemAppManagementService;

  @BeforeClass
  public static void setupClass() {
    Map<BootstrapStep.Type, BootstrapStepExecutor> executors = new HashMap<>();
    executors.put(STEP1.getType(), EXECUTOR1);
    executors.put(STEP2.getType(), EXECUTOR2);
    executors.put(STEP3.getType(), EXECUTOR3);

    List<BootstrapStep> steps = new ArrayList<>(3);
    steps.add(STEP1);
    steps.add(STEP2);
    steps.add(STEP3);
    bootstrapConfig = new BootstrapConfig(steps);
    BootstrapConfigProvider bootstrapConfigProvider = new InMemoryBootstrapConfigProvider(bootstrapConfig);

    bootstrapStore = AppFabricTestHelper.getInjector().getInstance(BootstrapStore.class);
    systemAppManagementService = AppFabricTestHelper.getInjector().getInstance(SystemAppManagementService.class);
    CapabilityManagementService capabilityManagementService = AppFabricTestHelper.getInjector()
      .getInstance(CapabilityManagementService.class);
    bootstrapService = new BootstrapService(bootstrapConfigProvider, bootstrapStore, executors,
                                            systemAppManagementService, capabilityManagementService);
    bootstrapService.reload();
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  @After
  public void cleanupTest() {
    bootstrapStore.clear();
    EXECUTOR1.reset();
    EXECUTOR2.reset();
    EXECUTOR3.reset();
  }

  @Test
  public void testAllSuccess() throws Exception  {
    BootstrapResult result = bootstrapService.bootstrap();

    List<BootstrapStepResult> expectedStepResults = new ArrayList<>();
    for (BootstrapStep step : bootstrapConfig.getSteps()) {
      expectedStepResults.add(new BootstrapStepResult(step.getLabel(), BootstrapStepResult.Status.SUCCEEDED));
    }
    BootstrapResult expected = new BootstrapResult(expectedStepResults);
    Assert.assertEquals(expected, result);
    Assert.assertTrue(bootstrapService.isBootstrapped());
  }

  @Test
  public void testRunCondition() throws Exception {
    BootstrapResult result =
      bootstrapService.bootstrap(step -> step.getRunCondition() == BootstrapStep.RunCondition.ONCE);

    List<BootstrapStepResult> stepResults = new ArrayList<>(3);
    stepResults.add(new BootstrapStepResult(STEP1.getLabel(), BootstrapStepResult.Status.SKIPPED));
    stepResults.add(new BootstrapStepResult(STEP2.getLabel(), BootstrapStepResult.Status.SUCCEEDED));
    stepResults.add(new BootstrapStepResult(STEP3.getLabel(), BootstrapStepResult.Status.SKIPPED));
    Assert.assertEquals(new BootstrapResult(stepResults), result);
  }

  @Test
  public void testRetries() throws Exception {
    EXECUTOR1.setNumRetryableFailures(3);
    BootstrapResult result = bootstrapService.bootstrap();

    List<BootstrapStepResult> expectedStepResults = new ArrayList<>();
    for (BootstrapStep step : bootstrapConfig.getSteps()) {
      expectedStepResults.add(new BootstrapStepResult(step.getLabel(), BootstrapStepResult.Status.SUCCEEDED));
    }
    BootstrapResult expected = new BootstrapResult(expectedStepResults);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testContinuesAfterFailures() throws Exception {
    EXECUTOR1.shouldFail();
    BootstrapResult result = bootstrapService.bootstrap();

    List<BootstrapStepResult> stepResults = new ArrayList<>(3);
    stepResults.add(new BootstrapStepResult(STEP1.getLabel(), BootstrapStepResult.Status.FAILED));
    stepResults.add(new BootstrapStepResult(STEP2.getLabel(), BootstrapStepResult.Status.SUCCEEDED));
    stepResults.add(new BootstrapStepResult(STEP3.getLabel(), BootstrapStepResult.Status.SUCCEEDED));
    BootstrapResult expected = new BootstrapResult(stepResults);
    Assert.assertEquals(expected, result);
  }

  @Test
  public void testNoConcurrentBootstrap() throws Exception {
    CountDownLatch runningLatch = new CountDownLatch(1);
    CountDownLatch waitingLatch = new CountDownLatch(1);
    EXECUTOR1.setLatches(runningLatch, waitingLatch);

    Thread t = new Thread(() -> {
      try {
        bootstrapService.bootstrap();
      } catch (InterruptedException e) {
        runningLatch.countDown();
      }
    });
    t.start();
    runningLatch.await();

    try {
      bootstrapService.bootstrap();
      Assert.fail("BootstrapService should not allow concurrent bootstrap operations");
    } catch (IllegalStateException e) {
      // expected
    } finally {
      waitingLatch.countDown();
    }
  }

  @Test
  public void testInterruption() throws Exception {
    // use the first executor to make sure bootstrap is running
    CountDownLatch runningLatch = new CountDownLatch(1);
    CountDownLatch waitingLatch = new CountDownLatch(1);
    EXECUTOR1.setLatches(runningLatch, waitingLatch);
    EXECUTOR1.setNumRetryableFailures(Integer.MAX_VALUE);

    // start the bootstrap
    CountDownLatch doneLatch = new CountDownLatch(1);
    AtomicBoolean wasInterrupted = new AtomicBoolean(false);
    Thread t = new Thread(() -> {
      try {
        bootstrapService.bootstrap();
      } catch (InterruptedException e) {
        // expected
        wasInterrupted.set(true);
      } finally {
        doneLatch.countDown();
      }
    });
    t.start();

    // make sure the first executor is executing
    runningLatch.await();
    // let the executor proceed to constant retries
    waitingLatch.countDown();
    // interrupt bootstrap
    t.interrupt();

    // wait for bootstrap to return
    doneLatch.await();

    // make sure the state is not bootstrapped and an InterruptedException was thrown
    Assert.assertFalse(bootstrapService.isBootstrapped());
    Assert.assertTrue(wasInterrupted.get());
  }

  /**
   * A fake executor that can be configured to complete, fail, or fail in a retry-able fashion.
   */
  private static class FakeExecutor extends BaseStepExecutor<EmptyArguments> {
    private int numAttempts;
    private int numRetryableFailures;
    private boolean shouldFail;
    private CountDownLatch runningLatch;
    private CountDownLatch waitingLatch;

    FakeExecutor() {
      reset();
    }

    private void reset() {
      numAttempts = 0;
      numRetryableFailures = 0;
      shouldFail = false;
      runningLatch = null;
      waitingLatch = null;
    }

    private void setNumRetryableFailures(int numRetryableFailures) {
      this.numRetryableFailures = numRetryableFailures;
    }

    private void shouldFail() {
      this.shouldFail = true;
    }

    private void setLatches(CountDownLatch runningLatch, CountDownLatch waitingLatch) {
      this.runningLatch = runningLatch;
      this.waitingLatch = waitingLatch;
    }

    @Override
    public void execute(EmptyArguments arguments) throws Exception {
      try {
        if (runningLatch != null) {
          runningLatch.countDown();
          waitingLatch.await();
        }
        if (shouldFail) {
          throw new Exception();
        }
        if (numRetryableFailures > numAttempts) {
          throw new RetryableException();
        }
      } finally {
        numAttempts++;
      }
    }

    @Override
    protected RetryStrategy getRetryStrategy() {
      // always retry
      return super.getRetryStrategy();
    }
  }
}
