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

package io.cdap.cdap.client;

import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Fast unit tests for {@link ProgramClient}.
 */
public class ProgramClientTest {

  private static final ProgramId PROGRAM_ID =
      NamespaceId.DEFAULT.app("myApp").workflow("myWorkflow");

  @Test
  public void testWaitForStatusSucceedsWhenMatching() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    ProgramClient client = createClientWithStatusBehavior(
        () -> calls.incrementAndGet() == 1
            ? ProgramStatus.STOPPED.name()
            : ProgramStatus.RUNNING.name());
    client.waitForStatus(PROGRAM_ID, ProgramStatus.RUNNING, 2, TimeUnit.SECONDS);
    Assert.assertEquals(2, calls.get());
  }

  @Test
  public void testWaitForStatusPropagatesDeclaredCheckedExceptions() throws Exception {
    UnauthenticatedException unauth = new UnauthenticatedException("unauth");
    try {
      createClientThrowingInsideWaitTask(unauth)
          .waitForStatus(PROGRAM_ID, ProgramStatus.RUNNING, 2, TimeUnit.SECONDS);
      Assert.fail("Expected UnauthenticatedException");
    } catch (UnauthenticatedException actual) {
      Assert.assertSame(unauth, actual);
    }

    ProgramNotFoundException notFound = new ProgramNotFoundException(PROGRAM_ID);
    try {
      createClientThrowingInsideWaitTask(notFound)
          .waitForStatus(PROGRAM_ID, ProgramStatus.RUNNING, 2, TimeUnit.SECONDS);
      Assert.fail("Expected ProgramNotFoundException");
    } catch (ProgramNotFoundException actual) {
      Assert.assertSame(notFound, actual);
    }

    IOException ioFailure = new IOException("io failure");
    try {
      createClientThrowingInsideWaitTask(ioFailure)
          .waitForStatus(PROGRAM_ID, ProgramStatus.RUNNING, 2, TimeUnit.SECONDS);
      Assert.fail("Expected IOException");
    } catch (IOException actual) {
      Assert.assertSame(ioFailure, actual);
    }
  }

  @Test
  public void testWaitForStatusWrapsUndeclaredCheckedAndPropagatesUnchecked() throws Exception {
    UnauthorizedException unauthorized = new UnauthorizedException("forbidden");
    try {
      createClientThrowingInsideWaitTask(unauthorized)
          .waitForStatus(PROGRAM_ID, ProgramStatus.RUNNING, 2, TimeUnit.SECONDS);
      Assert.fail("Expected UnauthorizedException directly");
    } catch (UnauthorizedException actual) {
      Assert.assertSame(unauthorized, actual);
    }

    IllegalStateException unchecked = new IllegalStateException("unexpected state");
    try {
      createClientThrowingInsideWaitTask(unchecked)
          .waitForStatus(PROGRAM_ID, ProgramStatus.RUNNING, 2, TimeUnit.SECONDS);
      Assert.fail("Expected IllegalStateException directly");
    } catch (IllegalStateException actual) {
      Assert.assertSame(unchecked, actual);
    }

    Exception undeclaredChecked = new Exception("undeclared checked failure");
    try {
      createClientThrowingInsideWaitTask(undeclaredChecked)
          .waitForStatus(PROGRAM_ID, ProgramStatus.RUNNING, 2, TimeUnit.SECONDS);
      Assert.fail("Expected RuntimeException wrapping undeclared checked exception");
    } catch (RuntimeException actual) {
      Assert.assertSame(undeclaredChecked, actual.getCause());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T, E extends Throwable> T sneakyThrow(Throwable t) throws E {
    throw (E) t;
  }

  private interface StatusBehavior {
    String getStatus()
        throws IOException, ProgramNotFoundException, UnauthenticatedException,
        UnauthorizedException;
  }

  /**
   * Note: {@code ProgramClient.waitForStatus} evaluates {@code getStatus(program)} once eagerly for
   * the timeout message before invoking {@code Tasks.waitFor}. Returning {@code STOPPED} on the
   * first call and throwing on the second call ensures the exception is thrown inside the
   * {@code Callable} and caught as {@code ExecutionException}.
   */
  private ProgramClient createClientThrowingInsideWaitTask(Throwable toThrow) {
    AtomicInteger calls = new AtomicInteger();
    return createClientWithStatusBehavior(() -> {
      if (calls.incrementAndGet() == 1) {
        return ProgramStatus.STOPPED.name();
      }
      return sneakyThrow(toThrow);
    });
  }

  private ProgramClient createClientWithStatusBehavior(StatusBehavior behavior) {
    ClientConfig clientConfig = ClientConfig.builder()
        .setConnectionConfig(new ConnectionConfig("localhost", 11015, false))
        .build();
    return new ProgramClient(clientConfig) {
      @Override
      public String getStatus(ProgramId programId)
          throws IOException, ProgramNotFoundException, UnauthenticatedException,
          UnauthorizedException {
        return behavior.getStatus();
      }
    };
  }
}
