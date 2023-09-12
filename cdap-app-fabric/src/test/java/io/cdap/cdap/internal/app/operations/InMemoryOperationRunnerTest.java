/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.operations;


import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.operation.LongRunningOperation;
import io.cdap.cdap.common.operation.LongRunningOperationRequest;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.operation.InMemoryOperationRunner;
import io.cdap.cdap.internal.app.operation.OperationStatePublisher;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operationrun.OperationError;
import io.cdap.cdap.proto.operationrun.OperationType;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class InMemoryOperationRunnerTest {

  private static Injector injector;
  private InMemoryOperationRunner<MockOperationRequest> runner;
  private static final OperationStatePublisher mockPublisher = Mockito.mock(
      OperationStatePublisher.class);
  private static final OperationRunId runId = new OperationRunId("namespace", "run");


  @BeforeClass
  public static void setupClass() {
    CConfiguration cconf = CConfiguration.create();
    injector = AppFabricTestHelper.getInjector(cconf, new AbstractModule() {
      @Override
      protected void configure() {
        MapBinder<OperationType, LongRunningOperation> operationBinder =
            MapBinder.newMapBinder(binder(), OperationType.class, LongRunningOperation.class);
        operationBinder.addBinding(OperationType.SCM_PULL).to(MockOperation.class);
        bind(OperationStatePublisher.class).toInstance(mockPublisher);
      }
    });
  }

  @Before
  public void setupTest() {
    runner = injector.getInstance(new Key<InMemoryOperationRunner<MockOperationRequest>>() {
    });
    Mockito.reset(mockPublisher);
  }

  @Test
  public void testRun() throws Exception {
    LongRunningOperationRequest<MockOperationRequest> request = new LongRunningOperationRequest<MockOperationRequest>(
        runId, OperationType.SCM_PULL, new MockOperationRequest(1, null, false, 3)
    );

    runner.run(request);
    // wait for the run to be finished
    Thread.sleep(50);

    Assert.assertEquals(4, request.getOperationRequest().getCount().get());
    Mockito.verify(mockPublisher, Mockito.times(1)).publishRunning();
    Mockito.verify(mockPublisher, Mockito.times(1)).publishSuccess();
  }

  @Test
  public void testRunWithError() throws Exception {
    OperationError expectedError = new OperationError("error", Collections.emptyList());
    LongRunningOperationRequest<MockOperationRequest> request = new LongRunningOperationRequest<MockOperationRequest>(
        runId, OperationType.SCM_PULL, new MockOperationRequest(1, expectedError, false, 3)
    );

    runner.run(request);
    // wait for the run to be finished
    Thread.sleep(50);

    Assert.assertEquals(0, request.getOperationRequest().getCount().get());
    Mockito.verify(mockPublisher, Mockito.times(1)).publishRunning();
    Mockito.verify(mockPublisher, Mockito.times(1)).publishFailed(expectedError);
  }

  @Test
  public void testRunWithException() throws Exception {
    LongRunningOperationRequest<MockOperationRequest> request = new LongRunningOperationRequest<MockOperationRequest>(
        runId, OperationType.SCM_PULL, new MockOperationRequest(1, null, true, 3)
    );
    runner.run(request);
    // wait for the run to be finished
    Thread.sleep(50);

    Assert.assertEquals(0, request.getOperationRequest().getCount().get());
    Mockito.verify(mockPublisher, Mockito.times(1)).publishRunning();
    Mockito.verify(mockPublisher, Mockito.times(1))
        .publishFailed(Mockito.any(OperationError.class));
  }

  @Test
  public void testStop() throws Exception {
    LongRunningOperationRequest<MockOperationRequest> request = new LongRunningOperationRequest<MockOperationRequest>(
        runId, OperationType.SCM_PULL, new MockOperationRequest(100, null, false, 5)
    );

    runner.run(request);
    // wait so atleast two increment has been done
    Thread.sleep(120);
    runner.stop(0, TimeUnit.SECONDS);
    // wait to verify no more increment will bo done
    Thread.sleep(120);

    Assert.assertEquals(2, request.getOperationRequest().getCount().get());
    Mockito.verify(mockPublisher, Mockito.times(1)).publishRunning();
    Mockito.verify(mockPublisher, Mockito.never()).publishSuccess();
    Mockito.verify(mockPublisher, Mockito.times(1)).publishStopped();
  }
}
