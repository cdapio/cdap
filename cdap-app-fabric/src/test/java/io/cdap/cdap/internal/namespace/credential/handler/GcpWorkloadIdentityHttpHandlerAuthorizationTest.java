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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.NamespaceCredentialProvider;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.DefaultContextAccessEnforcer;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import java.util.Collections;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that GcpWorkloadIdentityHttpHandler#getIdentity enforces authorization,
 * matching its sibling create/delete/validate endpoints (which enforce a
 * NamespacePermission) and the read precedent CredentialProviderHttpHandler#getIdentity
 * (which enforces StandardPermission.GET). Before the fix, getIdentity performed no
 * authorization check and additionally called switchToInternalUser() before reading, so
 * any caller could read another namespace's configured GCP service account.
 */
public class GcpWorkloadIdentityHttpHandlerAuthorizationTest {

  private static final Principal MASTER_PRINCIPAL =
      new Principal("master", Principal.PrincipalType.USER);
  private static final Principal UNPRIVILEGED_PRINCIPAL =
      new Principal("unprivileged", Principal.PrincipalType.USER);
  private static final NamespaceId VICTIM_NAMESPACE = new NamespaceId("victim-tenant-namespace");
  private static final String VICTIM_SERVICE_ACCOUNT =
      "victim-tenant-namespace-runtime@victim-project.iam.gserviceaccount.com";

  private GcpWorkloadIdentityHttpHandler handler;
  private HttpRequest request;
  private HttpResponder responder;

  @Before
  public void setUp() throws Exception {
    InMemoryAccessController inMemoryAccessController = new InMemoryAccessController();
    // Only the master principal is granted GET on the victim namespace.
    inMemoryAccessController.grant(Authorizable.fromEntityId(VICTIM_NAMESPACE), MASTER_PRINCIPAL,
        Collections.<Permission>singleton(StandardPermission.GET));

    ContextAccessEnforcer accessEnforcer =
        new DefaultContextAccessEnforcer(new AuthenticationTestContext(), inMemoryAccessController);

    NamespaceQueryAdmin namespaceQueryAdmin = mock(NamespaceQueryAdmin.class);
    NamespaceMeta namespaceMeta = mock(NamespaceMeta.class);
    when(namespaceMeta.getNamespaceId()).thenReturn(VICTIM_NAMESPACE);
    when(namespaceQueryAdmin.get(VICTIM_NAMESPACE)).thenReturn(namespaceMeta);

    CredentialIdentityManager credentialIdentityManager = mock(CredentialIdentityManager.class);
    CredentialIdentity identity =
        new CredentialIdentity("system", "gcp-wi-profile", "identity", VICTIM_SERVICE_ACCOUNT);
    when(credentialIdentityManager.get(any())).thenReturn(Optional.of(identity));

    NamespaceCredentialProvider credentialProvider = mock(NamespaceCredentialProvider.class);
    MetricsCollectionService metricsCollectionService = mock(MetricsCollectionService.class);

    handler = new GcpWorkloadIdentityHttpHandler(accessEnforcer, namespaceQueryAdmin,
        credentialIdentityManager, credentialProvider, metricsCollectionService);

    request = mock(HttpRequest.class);
    responder = mock(HttpResponder.class);
  }

  @Test
  public void testGetIdentityUnauthorizedIsRejected() {
    AuthenticationTestContext.actAsPrincipal(UNPRIVILEGED_PRINCIPAL);
    try {
      handler.getIdentity(request, responder, VICTIM_NAMESPACE.getNamespace());
      Assert.fail("Expected UnauthorizedException: an unprivileged caller must not be able to read "
          + "another namespace's workload identity.");
    } catch (UnauthorizedException e) {
      // Expected: the caller lacks GET on the victim namespace.
    } catch (Exception e) {
      Assert.fail("Expected UnauthorizedException but got: " + e);
    }
  }

  @Test
  public void testGetIdentityAuthorizedSucceeds() throws Exception {
    AuthenticationTestContext.actAsPrincipal(MASTER_PRINCIPAL);
    // Should not throw: the master principal has GET on the victim namespace.
    handler.getIdentity(request, responder, VICTIM_NAMESPACE.getNamespace());
  }
}
