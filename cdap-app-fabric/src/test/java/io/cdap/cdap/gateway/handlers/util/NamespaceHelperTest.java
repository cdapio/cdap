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

package io.cdap.cdap.gateway.handlers.util;

import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link NamespaceHelper}.
 */
public class NamespaceHelperTest {

  @Test
  public void testValidateNamespaceSuccessAndNotFound() throws Exception {
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    NamespaceId validNs = new NamespaceId("ns1");
    Mockito.when(queryAdmin.get(validNs))
        .thenReturn(new NamespaceMeta.Builder().setName(validNs).build());

    Assert.assertEquals(validNs, NamespaceHelper.validateNamespace(queryAdmin, "ns1"));
    Assert.assertThrows(IllegalArgumentException.class,
        () -> NamespaceHelper.validateNamespace(queryAdmin, "bad$ns"));

    NamespaceId missingNs = new NamespaceId("missing");
    NamespaceNotFoundException expectedNotFound = new NamespaceNotFoundException(missingNs);
    Mockito.when(queryAdmin.get(missingNs)).thenThrow(expectedNotFound);
    try {
      NamespaceHelper.validateNamespace(queryAdmin, "missing");
      Assert.fail("Expected NamespaceNotFoundException");
    } catch (NamespaceNotFoundException actual) {
      Assert.assertSame(expectedNotFound, actual);
    }
  }

  @Test
  public void testValidateNamespacePropagatesRuntimeExceptionAndWrapsCheckedException()
      throws Exception {
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);

    UnauthorizedException expectedRuntime = new UnauthorizedException("forbidden");
    Mockito.when(queryAdmin.get(new NamespaceId("restricted"))).thenThrow(expectedRuntime);
    try {
      NamespaceHelper.validateNamespace(queryAdmin, "restricted");
      Assert.fail("Expected UnauthorizedException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    IOException ioException = new IOException("http transport error");
    Mockito.when(queryAdmin.get(new NamespaceId("ioNs"))).thenThrow(ioException);
    RuntimeException wrapped = Assert.assertThrows(
        RuntimeException.class, () -> NamespaceHelper.validateNamespace(queryAdmin, "ioNs"));
    Assert.assertSame(ioException, wrapped.getCause());
  }
}
