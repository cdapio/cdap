/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.security.store;

import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests that listing secure keys in the {@code system} namespace requires instance {@code UPDATE}.
 */
public class DefaultSecureStoreServiceAuthorizationTest {

  @Test
  public void testSystemNamespaceListRequiresInstanceUpdate() throws Exception {
    Principal accessor = new Principal("accessor", Principal.PrincipalType.USER);
    Principal admin = new Principal("admin", Principal.PrincipalType.USER);

    InMemoryAccessController accessController = new InMemoryAccessController();
    accessController.grant(
        Authorizable.fromEntityId(NamespaceId.SYSTEM, EntityType.SECUREKEY),
        accessor, Collections.singleton(StandardPermission.LIST));
    accessController.grant(
        Authorizable.fromEntityId(InstanceId.SELF),
        accessor, Collections.singleton(StandardPermission.GET));
    accessController.grant(
        Authorizable.fromEntityId(NamespaceId.DEFAULT, EntityType.SECUREKEY),
        accessor, Collections.singleton(StandardPermission.LIST));
    accessController.grant(
        Authorizable.fromEntityId(InstanceId.SELF),
        admin, Collections.singleton(StandardPermission.UPDATE));

    SecureStoreService delegate = Mockito.mock(SecureStoreService.class);
    Mockito.when(delegate.list(Mockito.anyString())).thenReturn(Collections.emptyList());
    DefaultSecureStoreService service = new DefaultSecureStoreService(
        accessController, new AuthenticationTestContext(), delegate);

    AuthenticationTestContext.actAsPrincipal(accessor);
    assertUnauthorized(() -> service.list("system"));
    assertUnauthorized(() -> service.list("SYSTEM"));
    Mockito.verifyNoInteractions(delegate);

    // User namespace list still uses SECUREKEY LIST on the namespace.
    Assert.assertTrue(service.list("default").isEmpty());
    Mockito.verify(delegate).list("default");

    // Admin with InstanceId.SELF UPDATE can list system secure keys.
    AuthenticationTestContext.actAsPrincipal(admin);
    Assert.assertTrue(service.list("system").isEmpty());
    Mockito.verify(delegate).list("system");
  }

  private static void assertUnauthorized(ThrowingRunnable runnable) throws Exception {
    try {
      runnable.run();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
