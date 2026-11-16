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

package io.cdap.cdap.gateway.handlers;

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractAppLifecycleHttpHandler}.
 */
public class AbstractAppLifecycleHttpHandlerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private TestAppLifecycleHandler createHandler(NamespaceQueryAdmin queryAdmin) throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    return new TestAppLifecycleHandler(
        cConf,
        queryAdmin,
        Mockito.mock(ProgramRuntimeService.class),
        Mockito.mock(ApplicationLifecycleService.class));
  }

  @Test
  public void testValidateNamespaceValidSystemAndErrorCases() throws Exception {
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    NamespaceId validNs = new NamespaceId("ns1");
    Mockito.when(queryAdmin.get(validNs))
        .thenReturn(new NamespaceMeta.Builder().setName(validNs).build());

    TestAppLifecycleHandler handler = createHandler(queryAdmin);

    Assert.assertEquals(NamespaceId.SYSTEM, handler.checkNamespace("system"));
    Mockito.verify(queryAdmin, Mockito.never()).get(NamespaceId.SYSTEM);

    Assert.assertEquals(validNs, handler.checkNamespace("ns1"));
    Assert.assertThrows(BadRequestException.class, () -> handler.checkNamespace(null));
    Assert.assertThrows(BadRequestException.class, () -> handler.checkNamespace("invalid$ns"));

    NamespaceNotFoundException notFound = new NamespaceNotFoundException(new NamespaceId("missing"));
    Mockito.when(queryAdmin.get(new NamespaceId("missing"))).thenThrow(notFound);
    try {
      handler.checkNamespace("missing");
      Assert.fail("Expected NamespaceNotFoundException");
    } catch (NamespaceNotFoundException actual) {
      Assert.assertSame(notFound, actual);
    }

    UnauthorizedException unauthorized = new UnauthorizedException("denied");
    Mockito.when(queryAdmin.get(new NamespaceId("restricted"))).thenThrow(unauthorized);
    try {
      handler.checkNamespace("restricted");
      Assert.fail("Expected UnauthorizedException");
    } catch (AccessException actual) {
      Assert.assertSame(unauthorized, actual);
    }
  }

  @Test
  public void testValidateNamespacePropagatesRuntimeExceptionAndWrapsCheckedException()
      throws Exception {
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    TestAppLifecycleHandler handler = createHandler(queryAdmin);

    IllegalStateException expectedRuntime = new IllegalStateException("unexpected runtime");
    Mockito.when(queryAdmin.get(new NamespaceId("nsRuntime"))).thenThrow(expectedRuntime);
    try {
      handler.checkNamespace("nsRuntime");
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    IOException ioException = new IOException("remote namespace call failed");
    Mockito.when(queryAdmin.get(new NamespaceId("nsIo"))).thenThrow(ioException);
    RuntimeException wrapped = Assert.assertThrows(
        RuntimeException.class, () -> handler.checkNamespace("nsIo"));
    Assert.assertSame(ioException, wrapped.getCause());
  }

  private static final class TestAppLifecycleHandler extends AbstractAppLifecycleHttpHandler {

    private TestAppLifecycleHandler(CConfiguration configuration,
        NamespaceQueryAdmin namespaceQueryAdmin,
        ProgramRuntimeService runtimeService,
        ApplicationLifecycleService applicationLifecycleService) {
      super(configuration, namespaceQueryAdmin, runtimeService, applicationLifecycleService);
    }

    private NamespaceId checkNamespace(@Nullable String namespace)
        throws BadRequestException, NamespaceNotFoundException, AccessException {
      return validateNamespace(namespace);
    }
  }
}
