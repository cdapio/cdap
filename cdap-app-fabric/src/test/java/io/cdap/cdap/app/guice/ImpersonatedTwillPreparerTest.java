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
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ImpersonatedTwillPreparer}.
 */
public class ImpersonatedTwillPreparerTest {

  private static final ProgramId PROGRAM_ID =
      NamespaceId.DEFAULT.app("app").program(ProgramType.SERVICE, "svc");

  @Test
  public void testStartWrapsReturnedControllerWithImpersonation() {
    Configuration hConf = new Configuration();
    TwillPreparer delegate = Mockito.mock(TwillPreparer.class);
    TwillController rawController = Mockito.mock(TwillController.class);
    Mockito.when(delegate.start(30L, TimeUnit.SECONDS)).thenReturn(rawController);
    TokenSecureStoreRenewer renewer = Mockito.mock(TokenSecureStoreRenewer.class);
    AtomicReference<NamespacedEntityId> capturedEntity = new AtomicReference<>();
    Impersonator impersonator = createImpersonator(capturedEntity, null);

    ImpersonatedTwillPreparer preparer = new ImpersonatedTwillPreparer(
        hConf, delegate, impersonator, renewer, PROGRAM_ID);
    Assert.assertSame(delegate, preparer.getDelegate());

    TwillController result = preparer.start(30L, TimeUnit.SECONDS);
    Assert.assertTrue(result instanceof ImpersonatedTwillController);
    Assert.assertEquals(PROGRAM_ID, capturedEntity.get());

    result.kill();
    Mockito.verify(rawController).kill();
  }

  @Test
  public void testStartPropagatesRuntimeExceptionUnwrapped() {
    Configuration hConf = new Configuration();
    TwillPreparer delegate = Mockito.mock(TwillPreparer.class);
    IllegalArgumentException expected = new IllegalArgumentException("invalid launch config");
    Mockito.when(delegate.start(10L, TimeUnit.SECONDS)).thenThrow(expected);
    TokenSecureStoreRenewer renewer = Mockito.mock(TokenSecureStoreRenewer.class);
    Impersonator impersonator = createImpersonator(new AtomicReference<>(), null);

    ImpersonatedTwillPreparer preparer = new ImpersonatedTwillPreparer(
        hConf, delegate, impersonator, renewer, PROGRAM_ID);
    try {
      preparer.start(10L, TimeUnit.SECONDS);
      Assert.fail("Expected IllegalArgumentException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testStartWrapsCheckedIoException() {
    Configuration hConf = new Configuration();
    TwillPreparer delegate = Mockito.mock(TwillPreparer.class);
    TokenSecureStoreRenewer renewer = Mockito.mock(TokenSecureStoreRenewer.class);
    IOException ioException = new IOException("keytab lookup failed");
    Impersonator impersonator = createImpersonator(new AtomicReference<>(), ioException);

    ImpersonatedTwillPreparer preparer = new ImpersonatedTwillPreparer(
        hConf, delegate, impersonator, renewer, PROGRAM_ID);
    RuntimeException thrown = Assert.assertThrows(
        RuntimeException.class, () -> preparer.start(10L, TimeUnit.SECONDS));
    Assert.assertSame(ioException, thrown.getCause());
    Mockito.verify(delegate, Mockito.never()).start(Mockito.anyLong(), Mockito.any());
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
