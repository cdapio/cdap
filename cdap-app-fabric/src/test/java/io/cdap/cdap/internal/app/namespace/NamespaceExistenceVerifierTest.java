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

package io.cdap.cdap.internal.app.namespace;

import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link NamespaceExistenceVerifier}.
 */
public class NamespaceExistenceVerifierTest {

  @Test
  public void testSystemNamespaceSkipsAdminLookup() throws Exception {
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    NamespaceExistenceVerifier verifier = new NamespaceExistenceVerifier(queryAdmin);

    verifier.ensureExists(NamespaceId.SYSTEM);
    Mockito.verify(queryAdmin, Mockito.never()).exists(Mockito.any());
  }

  @Test
  public void testExistingAndMissingNamespace() throws Exception {
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    NamespaceId existingNs = new NamespaceId("existing_ns");
    NamespaceId missingNs = new NamespaceId("missing_ns");
    Mockito.when(queryAdmin.exists(existingNs)).thenReturn(true);
    Mockito.when(queryAdmin.exists(missingNs)).thenReturn(false);

    NamespaceExistenceVerifier verifier = new NamespaceExistenceVerifier(queryAdmin);
    verifier.ensureExists(existingNs);
    Assert.assertThrows(
        NamespaceNotFoundException.class, () -> verifier.ensureExists(missingNs));
  }

  @Test
  public void testPropagatesRuntimeExceptionUnwrapped() throws Exception {
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    NamespaceId ns = new NamespaceId("ns1");
    UnauthorizedException expected = new UnauthorizedException("unauthorized");
    Mockito.when(queryAdmin.exists(ns)).thenThrow(expected);

    NamespaceExistenceVerifier verifier = new NamespaceExistenceVerifier(queryAdmin);
    try {
      verifier.ensureExists(ns);
      Assert.fail("Expected UnauthorizedException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expected, actual);
    }
  }

  @Test
  public void testWrapsCheckedExceptionInRuntimeException() throws Exception {
    NamespaceQueryAdmin queryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    NamespaceId ns = new NamespaceId("ns1");
    IOException ioException = new IOException("http error");
    Mockito.when(queryAdmin.exists(ns)).thenThrow(ioException);

    NamespaceExistenceVerifier verifier = new NamespaceExistenceVerifier(queryAdmin);
    RuntimeException thrown =
        Assert.assertThrows(RuntimeException.class, () -> verifier.ensureExists(ns));
    Assert.assertSame(ioException, thrown.getCause());
  }
}
