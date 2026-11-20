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

import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.TokenSecureStoreRenewer;
import io.cdap.cdap.security.impersonation.ImpersonatedOpType;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.api.security.SecureStoreWriter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ImpersonatedTwillRunnerService}.
 */
public class ImpersonatedTwillRunnerServiceTest {

  private static final ProgramId PROGRAM_ID =
      NamespaceId.DEFAULT.app("myApp").program(ProgramType.SPARK, "mySpark");

  @Test
  public void testSecureStoreRenewerImpersonatesProgramOwner() throws IOException {
    TwillRunnerService delegate = Mockito.mock(TwillRunnerService.class);
    TokenSecureStoreRenewer tokenRenewer = Mockito.mock(TokenSecureStoreRenewer.class);
    AtomicReference<NamespacedEntityId> capturedEntity = new AtomicReference<>();
    Impersonator impersonator = createImpersonator(capturedEntity, null);

    ImpersonatedTwillRunnerService service = new ImpersonatedTwillRunnerService(
        new Configuration(), delegate, impersonator, tokenRenewer);

    SecureStoreRenewer rawRenewer = Mockito.mock(SecureStoreRenewer.class);
    service.setSecureStoreRenewer(rawRenewer, 1L, 2L, 3L, TimeUnit.SECONDS);

    ArgumentCaptor<SecureStoreRenewer> captor = ArgumentCaptor.forClass(SecureStoreRenewer.class);
    Mockito.verify(delegate).setSecureStoreRenewer(
        captor.capture(), Mockito.eq(1L), Mockito.eq(2L), Mockito.eq(3L),
        Mockito.eq(TimeUnit.SECONDS));

    SecureStoreRenewer wrappedRenewer = captor.getValue();
    RunId runId = RunIds.generate();
    SecureStoreWriter writer = Mockito.mock(SecureStoreWriter.class);
    String twillAppName = TwillAppNames.toTwillAppName(PROGRAM_ID);

    wrappedRenewer.renew(twillAppName, runId, writer);
    Assert.assertEquals(PROGRAM_ID, capturedEntity.get());
    Mockito.verify(rawRenewer).renew(twillAppName, runId, writer);
  }

  @Test
  public void testSecureStoreRenewerPropagatesRuntimeExceptionUnwrapped() throws IOException {
    TwillRunnerService delegate = Mockito.mock(TwillRunnerService.class);
    TokenSecureStoreRenewer tokenRenewer = Mockito.mock(TokenSecureStoreRenewer.class);
    Impersonator impersonator = createImpersonator(new AtomicReference<>(), null);
    ImpersonatedTwillRunnerService service = new ImpersonatedTwillRunnerService(
        new Configuration(), delegate, impersonator, tokenRenewer);

    IllegalStateException expected = new IllegalStateException("renew runtime failure");
    SecureStoreRenewer rawRenewer = Mockito.mock(SecureStoreRenewer.class);
    Mockito.doThrow(expected).when(rawRenewer)
        .renew(Mockito.anyString(), Mockito.any(), Mockito.any());

    service.setSecureStoreRenewer(rawRenewer, 1L, 2L, 3L, TimeUnit.SECONDS);
    ArgumentCaptor<SecureStoreRenewer> captor = ArgumentCaptor.forClass(SecureStoreRenewer.class);
    Mockito.verify(delegate).setSecureStoreRenewer(
        captor.capture(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());

    String twillAppName = TwillAppNames.toTwillAppName(PROGRAM_ID);
    try {
      captor.getValue().renew(twillAppName, RunIds.generate(), Mockito.mock(SecureStoreWriter.class));
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testSecureStoreRenewerWrapsCheckedIoExceptionFromImpersonation() throws IOException {
    TwillRunnerService delegate = Mockito.mock(TwillRunnerService.class);
    TokenSecureStoreRenewer tokenRenewer = Mockito.mock(TokenSecureStoreRenewer.class);
    IOException ioException = new IOException("impersonation failure");
    Impersonator impersonator = createImpersonator(new AtomicReference<>(), ioException);
    ImpersonatedTwillRunnerService service = new ImpersonatedTwillRunnerService(
        new Configuration(), delegate, impersonator, tokenRenewer);

    SecureStoreRenewer rawRenewer = Mockito.mock(SecureStoreRenewer.class);
    service.setSecureStoreRenewer(rawRenewer, 1L, 2L, 3L, TimeUnit.SECONDS);
    ArgumentCaptor<SecureStoreRenewer> captor = ArgumentCaptor.forClass(SecureStoreRenewer.class);
    Mockito.verify(delegate).setSecureStoreRenewer(
        captor.capture(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());

    String twillAppName = TwillAppNames.toTwillAppName(PROGRAM_ID);
    RuntimeException thrown = Assert.assertThrows(RuntimeException.class,
        () -> captor.getValue().renew(
            twillAppName, RunIds.generate(), Mockito.mock(SecureStoreWriter.class)));
    Assert.assertSame(ioException, thrown.getCause());
    Mockito.verify(rawRenewer, Mockito.never())
        .renew(Mockito.anyString(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testMasterServiceAndNonProgramAppSkipImpersonation() throws IOException {
    TwillRunnerService delegate = Mockito.mock(TwillRunnerService.class);
    TwillController rawController = Mockito.mock(TwillController.class);
    RunId runId = RunIds.generate();
    Mockito.when(delegate.lookup(Mockito.anyString(), Mockito.eq(runId))).thenReturn(rawController);

    AtomicReference<NamespacedEntityId> capturedEntity = new AtomicReference<>();
    Impersonator impersonator = createImpersonator(capturedEntity, null);
    ImpersonatedTwillRunnerService service = new ImpersonatedTwillRunnerService(
        new Configuration(), delegate, impersonator, Mockito.mock(TokenSecureStoreRenewer.class));

    Assert.assertSame(rawController, service.lookup(Constants.Service.MASTER_SERVICES, runId));
    Assert.assertSame(rawController, service.lookup("non-program-app", runId));
    Assert.assertTrue(
        service.lookup(TwillAppNames.toTwillAppName(PROGRAM_ID), runId)
            instanceof ImpersonatedTwillController);

    SecureStoreRenewer rawRenewer = Mockito.mock(SecureStoreRenewer.class);
    service.setSecureStoreRenewer(rawRenewer, 1L, 2L, 3L, TimeUnit.SECONDS);
    ArgumentCaptor<SecureStoreRenewer> captor = ArgumentCaptor.forClass(SecureStoreRenewer.class);
    Mockito.verify(delegate).setSecureStoreRenewer(
        captor.capture(), Mockito.anyLong(), Mockito.anyLong(), Mockito.anyLong(), Mockito.any());

    SecureStoreWriter writer = Mockito.mock(SecureStoreWriter.class);
    captor.getValue().renew(Constants.Service.MASTER_SERVICES, runId, writer);
    captor.getValue().renew("non-program-app", runId, writer);
    Assert.assertNull(capturedEntity.get());
    Mockito.verify(rawRenewer).renew(Constants.Service.MASTER_SERVICES, runId, writer);
    Mockito.verify(rawRenewer).renew("non-program-app", runId, writer);
  }

  private static Impersonator createImpersonator(
      AtomicReference<NamespacedEntityId> capturedEntity, Exception failure) {
    return new Impersonator() {
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
    };
  }
}
