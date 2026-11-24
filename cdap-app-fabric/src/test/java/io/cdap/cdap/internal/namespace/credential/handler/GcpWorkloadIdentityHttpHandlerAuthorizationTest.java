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

package io.cdap.cdap.internal.namespace.credential.handler;

import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.NamespaceCredentialProvider;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Collections;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Authorization tests for {@link GcpWorkloadIdentityHttpHandler}.
 */
public class GcpWorkloadIdentityHttpHandlerAuthorizationTest {

  private static final Principal ALICE = new Principal("gcpwi-alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("gcpwi-bob", Principal.PrincipalType.USER);

  @Test
  public void testGetIdentity() throws Exception {
    NamespaceId namespace = new NamespaceId("gcpwins");
    InMemoryAccessController accessController = new InMemoryAccessController();
    accessController.grant(Authorizable.fromEntityId(namespace), ALICE,
        Collections.singleton(StandardPermission.GET));
    NamespaceQueryAdmin namespaceQueryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(namespaceQueryAdmin.get(namespace))
        .thenReturn(new NamespaceMeta.Builder().setName(namespace).build());
    CredentialIdentityManager identityManager = Mockito.mock(CredentialIdentityManager.class);
    Mockito.when(identityManager.get(Mockito.any())).thenReturn(Optional.of(
        new CredentialIdentity(NamespaceId.SYSTEM.getNamespace(), "profile", "identity", "sa")));
    GcpWorkloadIdentityHttpHandler handler = new GcpWorkloadIdentityHttpHandler(
        new DefaultContextAccessEnforcer(new AuthenticationTestContext(), accessController),
        namespaceQueryAdmin, identityManager, Mockito.mock(NamespaceCredentialProvider.class),
        new NoOpMetricsCollectionService());
    HttpResponder responder = Mockito.mock(HttpResponder.class);

    AuthenticationTestContext.actAsPrincipal(BOB);
    try {
      handler.getIdentity(null, responder, namespace.getNamespace());
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    Mockito.verifyNoInteractions(namespaceQueryAdmin, identityManager, responder);

    AuthenticationTestContext.actAsPrincipal(ALICE);
    handler.getIdentity(null, responder, namespace.getNamespace());
    Mockito.verify(responder).sendJson(Mockito.eq(HttpResponseStatus.OK), Mockito.anyString());
  }
}
