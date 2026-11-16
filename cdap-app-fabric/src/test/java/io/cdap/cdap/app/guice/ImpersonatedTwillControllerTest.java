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

package io.cdap.cdap.app.guice;

import com.google.common.util.concurrent.Futures;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.impersonation.ImpersonatedOpType;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ImpersonatedTwillController}.
 */
public class ImpersonatedTwillControllerTest {

  private static final ProgramId PROGRAM_ID =
      NamespaceId.DEFAULT.app("testApp").program(ProgramType.WORKER, "testWorker");

  @Test
  public void testKillSuccessWithImpersonation() {
    TwillController delegate = Mockito.mock(TwillController.class);
    AtomicReference<NamespacedEntityId> impersonatedEntity = new AtomicReference<>();
    Impersonator impersonator = new DirectImpersonator(impersonatedEntity, null);

    ImpersonatedTwillController controller =
        new ImpersonatedTwillController(delegate, impersonator, PROGRAM_ID);
    controller.kill();

    Assert.assertEquals(PROGRAM_ID, impersonatedEntity.get());
    Mockito.verify(delegate).kill();
  }

  @Test
  public void testKillPropagatesRuntimeExceptionUnwrapped() {
    TwillController delegate = Mockito.mock(TwillController.class);
    IllegalStateException expected = new IllegalStateException("kill failed");
    Mockito.doThrow(expected).when(delegate).kill();
    Impersonator impersonator = new DirectImpersonator(new AtomicReference<>(), null);

    ImpersonatedTwillController controller =
        new ImpersonatedTwillController(delegate, impersonator, PROGRAM_ID);
    try {
      controller.kill();
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testKillWrapsCheckedIoException() {
    TwillController delegate = Mockito.mock(TwillController.class);
    IOException ioException = new IOException("impersonation io failure");
    Impersonator impersonator = new DirectImpersonator(new AtomicReference<>(), ioException);

    ImpersonatedTwillController controller =
        new ImpersonatedTwillController(delegate, impersonator, PROGRAM_ID);
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class, controller::kill);
    Assert.assertSame(ioException, thrown.getCause());
    Mockito.verify(delegate, Mockito.never()).kill();
  }

  @Test
  public void testTerminateAndResourceReportImpersonation() throws Exception {
    TwillController delegate = Mockito.mock(TwillController.class);
    ResourceReport report = Mockito.mock(ResourceReport.class);
    Mockito.when(delegate.getResourceReport()).thenReturn(report);
    Mockito.doReturn(Futures.immediateFuture(delegate)).when(delegate).terminate();

    ImpersonatedTwillController controller = new ImpersonatedTwillController(
        delegate, new DirectImpersonator(new AtomicReference<>(), null), PROGRAM_ID);
    Assert.assertSame(report, controller.getResourceReport());
    Assert.assertSame(delegate, controller.terminate().get());

    IOException ioException = new IOException("ugi failure");
    ImpersonatedTwillController failingController = new ImpersonatedTwillController(
        delegate, new DirectImpersonator(new AtomicReference<>(), ioException), PROGRAM_ID);
    Assert.assertNull(failingController.getResourceReport());
    Future<? extends ServiceController> failedTerminate = failingController.terminate();
    ExecutionException execEx = Assert.assertThrows(ExecutionException.class, failedTerminate::get);
    Assert.assertSame(ioException, execEx.getCause());

    ServiceUnavailableException unavailable = new ServiceUnavailableException("master");
    ImpersonatedTwillController unavailableController = new ImpersonatedTwillController(
        delegate, new DirectImpersonator(new AtomicReference<>(), unavailable), PROGRAM_ID);
    Assert.assertNull(unavailableController.getResourceReport());
  }

  private static final class DirectImpersonator implements Impersonator {

    private final AtomicReference<NamespacedEntityId> capturedEntity;
    private final Exception failure;

    private DirectImpersonator(AtomicReference<NamespacedEntityId> capturedEntity,
        Exception failure) {
      this.capturedEntity = capturedEntity;
      this.failure = failure;
    }

    @Override
    public <T> T doAs(NamespacedEntityId entityId, Callable<T> callable) throws Exception {
      capturedEntity.set(entityId);
      if (failure != null) {
        throw failure;
      }
      return callable.call();
    }

    @Override
    public <T> T doAs(NamespacedEntityId entityId, Callable<T> callable,
        ImpersonatedOpType impersonatedOpType) throws Exception {
      return doAs(entityId, callable);
    }

    @Override
    public UserGroupInformation getUGI(NamespacedEntityId entityId) {
      throw new UnsupportedOperationException();
    }
  }
}
