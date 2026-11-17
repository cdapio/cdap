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

package io.cdap.cdap.internal.app.runtime;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.exception.WrappedStageException;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ProgramControllerServiceAdapter}.
 */
public class ProgramControllerServiceAdapterTest {

  private static ProgramRunId newRunId() {
    return NamespaceId.DEFAULT.app("app")
        .program(ProgramType.WORKER, "worker")
        .run(RunIds.generate());
  }

  @Test
  public void testNaturalServiceCompletionTransitionsToCompleted() throws Exception {
    Service shortLivedService = new AbstractExecutionThreadService() {
      @Override
      protected void run() {
        // completes immediately
      }
    };
    ProgramControllerServiceAdapter controller =
        new ProgramControllerServiceAdapter(shortLivedService, newRunId());

    shortLivedService.startAsync().awaitRunning();
    Tasks.waitFor(ProgramController.State.COMPLETED, controller::getState,
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testControllerStopAndGracefulStopTransitionToKilled() throws Exception {
    Service idleService = new SimpleIdleService();
    ProgramControllerServiceAdapter controller =
        new ProgramControllerServiceAdapter(idleService, newRunId());

    idleService.startAsync().awaitRunning();
    Tasks.waitFor(ProgramController.State.ALIVE, controller::getState,
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

    controller.stop().get(5, TimeUnit.SECONDS);
    Assert.assertEquals(ProgramController.State.KILLED, controller.getState());
    Assert.assertEquals(Service.State.TERMINATED, idleService.state());

    Service gracefulService = new SimpleIdleService();
    AtomicLong capturedGracefulTimeout = new AtomicLong(-1L);
    ProgramControllerServiceAdapter gracefulController =
        new ProgramControllerServiceAdapter(gracefulService, newRunId()) {
          @Override
          protected void gracefulStop(long gracefulTimeoutMillis) {
            capturedGracefulTimeout.set(gracefulTimeoutMillis);
            super.gracefulStop(gracefulTimeoutMillis);
          }
        };

    gracefulService.startAsync().awaitRunning();
    Tasks.waitFor(ProgramController.State.ALIVE, gracefulController::getState,
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

    gracefulController.stop(3L, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS);
    Assert.assertEquals(3000L, capturedGracefulTimeout.get());
    Assert.assertEquals(ProgramController.State.KILLED, gracefulController.getState());
    Assert.assertEquals(Service.State.TERMINATED, gracefulService.state());
  }

  @Test
  public void testExternalServiceStopTransitionsControllerToKilled() throws Exception {
    Service idleService = new SimpleIdleService();
    ProgramControllerServiceAdapter controller =
        new ProgramControllerServiceAdapter(idleService, newRunId());

    idleService.startAsync().awaitRunning();
    Tasks.waitFor(ProgramController.State.ALIVE, controller::getState,
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

    idleService.stopAsync().awaitTerminated();
    Tasks.waitFor(ProgramController.State.KILLED, controller::getState,
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testServiceFailureTransitionsControllerToError() throws Exception {
    WrappedStageException stageException =
        new WrappedStageException(new IllegalArgumentException("bad record"), "stageA");
    RuntimeException outerFailure = new RuntimeException("pipeline failed", stageException);

    Service failingService = new AbstractExecutionThreadService() {
      @Override
      protected void run() {
        throw outerFailure;
      }
    };
    ProgramControllerServiceAdapter controller =
        new ProgramControllerServiceAdapter(failingService, newRunId());

    failingService.startAsync();
    Tasks.waitFor(ProgramController.State.ERROR, controller::getState,
        5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);
    Assert.assertSame(outerFailure, controller.getFailureCause());
  }

  private static final class SimpleIdleService extends AbstractIdleService {

    @Override
    protected void startUp() {
      // no-op
    }

    @Override
    protected void shutDown() {
      // no-op
    }
  }
}
