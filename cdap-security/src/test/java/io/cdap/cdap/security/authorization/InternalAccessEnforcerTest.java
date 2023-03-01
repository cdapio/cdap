/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.Set;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the {@link InternalAccessEnforcer}.
 */
public class InternalAccessEnforcerTest {
  private static final long MINUTE_MILLIS = 60 * 1000;
  private static final String SYSTEM_PRINCIPAL = "system";

  private Injector injector;
  private TokenManager tokenManager;
  private InternalAccessEnforcer internalAccessEnforcer;
  private Codec<AccessToken> accessTokenCodec;

  @Before
  public void setupInternalAccessEnforcer() {
    this.injector = Guice.createInjector(new IOModule(), new ConfigModule(),
                                         new CoreSecurityRuntimeModule().getInMemoryModules());
    this.tokenManager = injector.getInstance(TokenManager.class);
    this.accessTokenCodec = injector.getInstance(Key.get(new TypeLiteral<Codec<AccessToken>>() { }));
    this.tokenManager.startUp();
    this.internalAccessEnforcer = injector.getInstance(InternalAccessEnforcer.class);
  }

  @After
  public void teardownInternalAccessEnforcer() {
    tokenManager.shutDown();
  }

  @Test
  public void testInternalAccessEnforceSuccess() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforce(ns, principal, StandardPermission.GET);
  }

  @Test
  public void testInternalAccessEnforceOnParentSuccess() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforceOnParent(EntityType.APPLICATION, ns, principal, StandardPermission.GET);
  }

  @Test
  public void testInternalAccessIsVisibleSuccess() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Set<EntityId> entities = Collections.singleton(ns);
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    Assert.assertEquals(entities, internalAccessEnforcer.isVisible(entities, principal));
  }

  @Test(expected = IllegalStateException.class)
  public void testInternalAccessEnforceNonInternalCredentialType() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.EXTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforce(ns, principal, StandardPermission.GET);
  }

  @Test(expected = IllegalStateException.class)
  public void testInternalAccessEnforceOnParentNonInternalCredentialType() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.EXTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforceOnParent(EntityType.APPLICATION, ns, principal, StandardPermission.GET);
  }

  @Test(expected = IllegalStateException.class)
  public void testInternalAccessIsVisibleNonInternalCredentialType() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Set<EntityId> entities = Collections.singleton(ns);
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.EXTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.isVisible(entities, principal);
  }

  @Test(expected = AccessException.class)
  public void testInternalAccessEnforceNonInternalTokenType() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.EXTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforce(ns, principal, StandardPermission.GET);
  }

  @Test(expected = AccessException.class)
  public void testInternalAccessEnforceOnParentNonInternalTokenType() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.EXTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforceOnParent(EntityType.APPLICATION, ns, principal, StandardPermission.GET);
  }

  @Test
  public void testInternalAccessIsVisibleNonInternalTokenType() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Set<EntityId> entities = Collections.singleton(ns);
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.EXTERNAL,
                                                 Collections.emptyList(), currentTime,
                                                 currentTime + 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    Assert.assertEquals(Collections.emptySet(), internalAccessEnforcer.isVisible(entities, principal));
  }

  @Test(expected = IllegalStateException.class)
  public void testInternalAccessEnforceNullCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, null);
    internalAccessEnforcer.enforce(ns, principal, StandardPermission.GET);
  }

  @Test(expected = IllegalStateException.class)
  public void testInternalAccessEnforceOnParentNullCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, null);
    internalAccessEnforcer.enforceOnParent(EntityType.APPLICATION, ns, principal, StandardPermission.GET);
  }

  @Test(expected = IllegalStateException.class)
  public void testInternalAccessIsVisibleNullCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Set<EntityId> entities = Collections.singleton(ns);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, null);
    Assert.assertEquals(Collections.emptySet(), internalAccessEnforcer.isVisible(entities, principal));
  }

  @Test(expected = AccessException.class)
  public void testInternalAccessEnforceInvalidCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Credential credential = new Credential("invalid", Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforce(ns, principal, StandardPermission.GET);
  }

  @Test(expected = AccessException.class)
  public void testInternalAccessEnforceOnParentInvalidCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Credential credential = new Credential("invalid", Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforceOnParent(EntityType.APPLICATION, ns, principal, StandardPermission.GET);
  }

  @Test
  public void testInternalAccessIsVisibleInvalidCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Set<EntityId> entities = Collections.singleton(ns);
    Credential credential = new Credential("invalid", Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    Assert.assertEquals(Collections.emptySet(), internalAccessEnforcer.isVisible(entities, principal));
  }

  @Test(expected = AccessException.class)
  public void testInternalAccessEnforceExpiredCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime - 10 * MINUTE_MILLIS,
                                                 currentTime - 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforce(ns, principal, StandardPermission.GET);
  }

  @Test(expected = AccessException.class)
  public void testInternalAccessEnforceOnParentExpiredCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime - 10 * MINUTE_MILLIS,
                                                 currentTime - 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    internalAccessEnforcer.enforceOnParent(EntityType.APPLICATION, ns, principal, StandardPermission.GET);
  }

  @Test
  public void testInternalAccessIsVisibleExpiredCredential() throws IOException {
    NamespaceId ns = new NamespaceId("namespace");
    Set<EntityId> entities = Collections.singleton(ns);
    long currentTime = System.currentTimeMillis();
    UserIdentity userIdentity = new UserIdentity(SYSTEM_PRINCIPAL, UserIdentity.IdentifierType.INTERNAL,
                                                 Collections.emptyList(), currentTime - 10 * MINUTE_MILLIS,
                                                 currentTime - 5 * MINUTE_MILLIS);
    String encodedIdentity = Base64.getEncoder()
      .encodeToString(accessTokenCodec.encode(tokenManager.signIdentifier(userIdentity)));
    Credential credential = new Credential(encodedIdentity, Credential.CredentialType.INTERNAL);
    Principal principal = new Principal(SYSTEM_PRINCIPAL, Principal.PrincipalType.USER, null, credential);
    Assert.assertEquals(Collections.emptySet(), internalAccessEnforcer.isVisible(entities, principal));
  }
}
