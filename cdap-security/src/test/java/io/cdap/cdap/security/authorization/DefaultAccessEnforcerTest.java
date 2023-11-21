/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.metrics.ProgramTypeMetricTag;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.encryption.FakeAeadCipher;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.security.spi.encryption.CipherException;
import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link DefaultAccessEnforcer}.
 */
public class DefaultAccessEnforcerTest extends AuthorizationTestBase {

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId NS = new NamespaceId("ns");
  private static final ApplicationId APP = NS.app("app");
  private static FakeAeadCipher fakeAeadCipherService;

  private static class ControllerWrapper {

    private final AccessController accessController;
    private final DefaultAccessEnforcer defaultAccessEnforcer;
    private final MetricsContext mockMetricsContext;

    ControllerWrapper(AccessController accessController,
        DefaultAccessEnforcer defaultAccessEnforcer,
        MetricsContext mockMetricsContext) {
      this.accessController = accessController;
      this.defaultAccessEnforcer = defaultAccessEnforcer;
      this.mockMetricsContext = mockMetricsContext;
    }
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setupClass() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes()
        .put(Attributes.Name.MAIN_CLASS, InMemoryAccessController.class.getName());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(
        locationFactory, InMemoryAccessController.class, manifest);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    fakeAeadCipherService = new FakeAeadCipher();
    fakeAeadCipherService.initialize();
  }

  private static ControllerWrapper createControllerWrapper(CConfiguration cConf,
      SConfiguration sConf,
      AccessEnforcer internalAccessEnforcer) {
    MetricsCollectionService mockMetricsCollectionService = mock(MetricsCollectionService.class);
    MetricsContext mockMetricsContext = mock(MetricsContext.class);
    when(mockMetricsCollectionService.getContext(any(Map.class))).thenReturn(mockMetricsContext);
    AccessControllerInstantiator accessControllerInstantiator = new AccessControllerInstantiator(
        cConf,
        AUTH_CONTEXT_FACTORY);
    DefaultAccessEnforcer defaultAccessEnforcer = new DefaultAccessEnforcer(cConf, sConf,
        accessControllerInstantiator,
        internalAccessEnforcer,
        mockMetricsCollectionService, fakeAeadCipherService);
    return new ControllerWrapper(accessControllerInstantiator.get(), defaultAccessEnforcer,
        mockMetricsContext);
  }

  @Test
  public void testAuthenticationDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.ENABLED, false);
    verifyDisabled(cConfCopy);
  }

  @Test
  public void testAuthorizationDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.Authorization.ENABLED, false);
    verifyDisabled(cConfCopy);
  }

  @Test
  public void testPropagationDisabled() throws AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    ControllerWrapper controllerWrapper = createControllerWrapper(cConfCopy, SCONF, null);
    controllerWrapper.accessController.grant(Authorizable.fromEntityId(NS), ALICE,
        ImmutableSet.of(StandardPermission.UPDATE));
    DefaultAccessEnforcer accessEnforcer = controllerWrapper.defaultAccessEnforcer;
    accessEnforcer.enforce(NS, ALICE, StandardPermission.UPDATE);
    try {
      accessEnforcer.enforce(APP, ALICE, StandardPermission.UPDATE);
      Assert.fail("Alice should not have ADMIN privilege on the APP.");
    } catch (UnauthorizedException ignored) {
      // expected
    }
    // Verify the metrics context was called with correct metrics
    verify(controllerWrapper.mockMetricsContext, times(1))
        .increment(Constants.Metrics.Authorization.EXTENSION_CHECK_SUCCESS_COUNT, 1);
    verify(controllerWrapper.mockMetricsContext, times(1))
        .increment(Constants.Metrics.Authorization.EXTENSION_CHECK_FAILURE_COUNT, 1);
    verify(controllerWrapper.mockMetricsContext, times(2))
        .gauge(eq(Constants.Metrics.Authorization.EXTENSION_CHECK_MILLIS), any(Long.class));
  }

  @Test
  public void testAuthEnforce() throws IOException, AccessException {
    ControllerWrapper controllerWrapper = createControllerWrapper(CCONF, SCONF, null);
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer authEnforcementService = controllerWrapper.defaultAccessEnforcer;
    // update privileges for alice. Currently alice has not been granted any privileges.
    assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.UPDATE);

    // grant some test privileges
    DatasetId ds = NS.dataset("ds");
    accessController
        .grant(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(StandardPermission.GET,
            StandardPermission.UPDATE));
    accessController
        .grant(Authorizable.fromEntityId(ds), BOB, ImmutableSet.of(StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(NS, EntityType.DATASET), ALICE,
        ImmutableSet.of(StandardPermission.LIST));

    // auth enforcement for alice should succeed on ns for actions read, write and list datasets
    authEnforcementService
        .enforce(NS, ALICE, ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE));
    authEnforcementService.enforceOnParent(EntityType.DATASET, NS, ALICE, StandardPermission.LIST);
    assertAuthorizationFailure(authEnforcementService, NS, ALICE,
        EnumSet.allOf(StandardPermission.class));
    // alice do not have CREATE, READ or WRITE on the dataset, so authorization should fail
    assertAuthorizationFailure(authEnforcementService, ds, ALICE, StandardPermission.GET);
    assertAuthorizationFailure(authEnforcementService, ds, ALICE, StandardPermission.UPDATE);
    assertAuthorizationFailure(authEnforcementService, EntityType.DATASET, NS, ALICE,
        StandardPermission.CREATE);

    // Alice doesn't have Delete right on NS, hence should fail.
    assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.DELETE);
    // bob enforcement should succeed since we grant him admin privilege
    authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
    // revoke all of alice's privileges
    accessController
        .revoke(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(StandardPermission.GET));
    assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.GET);

    accessController.revoke(Authorizable.fromEntityId(NS));

    assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.GET);
    assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.UPDATE);
    authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
    // Verify the metrics context was called with correct metrics
    verify(controllerWrapper.mockMetricsContext, times(4))
        .increment(Constants.Metrics.Authorization.EXTENSION_CHECK_SUCCESS_COUNT, 1);
    verify(controllerWrapper.mockMetricsContext, times(9))
        .increment(Constants.Metrics.Authorization.EXTENSION_CHECK_FAILURE_COUNT, 1);
    verify(controllerWrapper.mockMetricsContext, times(13))
        .gauge(eq(Constants.Metrics.Authorization.EXTENSION_CHECK_MILLIS), any(Long.class));
  }

  @Test
  public void testIsVisible() throws AccessException {
    ControllerWrapper controllerWrapper = createControllerWrapper(CCONF, SCONF, null);
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer authEnforcementService = controllerWrapper.defaultAccessEnforcer;

    NamespaceId ns1 = new NamespaceId("ns1");
    NamespaceId ns2 = new NamespaceId("ns2");
    DatasetId ds11 = ns1.dataset("ds11");
    DatasetId ds12 = ns1.dataset("ds12");
    DatasetId ds21 = ns2.dataset("ds21");
    DatasetId ds22 = ns2.dataset("ds22");
    DatasetId ds23 = ns2.dataset("ds33");
    Set<NamespaceId> namespaces = ImmutableSet.of(ns1, ns2);
    // Alice has access on ns1, ns2, ds11, ds21, ds23, Bob has access on ds11, ds12, ds22
    accessController.grant(Authorizable.fromEntityId(ns1), ALICE,
        Collections.singleton(StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(ns2), ALICE,
        Collections.singleton(StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(ds11), ALICE,
        Collections.singleton(StandardPermission.GET));
    accessController.grant(Authorizable.fromEntityId(ds11), BOB,
        Collections.singleton(StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(ds21), ALICE,
        Collections.singleton(StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(ds12), BOB,
        Collections.singleton(StandardPermission.UPDATE));
    accessController
        .grant(Authorizable.fromEntityId(ds12), BOB, EnumSet.allOf(StandardPermission.class));
    accessController.grant(Authorizable.fromEntityId(ds21), ALICE,
        Collections.singleton(StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(ds23), ALICE,
        Collections.singleton(StandardPermission.UPDATE));
    accessController.grant(Authorizable.fromEntityId(ds22), BOB,
        Collections.singleton(StandardPermission.UPDATE));

    Assert.assertEquals(namespaces.size(),
        authEnforcementService.isVisible(namespaces, ALICE).size());
    // bob should also be able to list two namespaces since he has privileges on the dataset in both namespaces
    Assert
        .assertEquals(namespaces.size(), authEnforcementService.isVisible(namespaces, BOB).size());
    Set<DatasetId> expectedDatasetIds = ImmutableSet.of(ds11, ds21, ds23);
    Assert.assertEquals(expectedDatasetIds.size(),
        authEnforcementService.isVisible(expectedDatasetIds,
            ALICE).size());
    expectedDatasetIds = ImmutableSet.of(ds12, ds22);
    // this will be empty since now isVisible will not check the hierarchy privilege for the parent of the entity
    Assert.assertEquals(Collections.EMPTY_SET,
        authEnforcementService.isVisible(expectedDatasetIds, ALICE));
    expectedDatasetIds = ImmutableSet.of(ds11, ds12, ds22);
    Assert.assertEquals(expectedDatasetIds.size(),
        authEnforcementService.isVisible(expectedDatasetIds, BOB).size());
    expectedDatasetIds = ImmutableSet.of(ds21, ds23);
    Assert.assertTrue(authEnforcementService.isVisible(expectedDatasetIds, BOB).isEmpty());
    // Verify the metrics context was called with correct metrics
    verify(controllerWrapper.mockMetricsContext, times(6))
        .increment(Constants.Metrics.Authorization.NON_INTERNAL_VISIBILITY_CHECK_COUNT, 1);
    verify(controllerWrapper.mockMetricsContext, times(6))
        .gauge(eq(Constants.Metrics.Authorization.EXTENSION_VISIBILITY_MILLIS), any(Long.class));
  }

  @Test
  public void testAuthEnforceWithEncryptedCredential() throws AccessException, CipherException {
    SConfiguration sConfCopy = enableCredentialEncryption();
    String cred = fakeAeadCipherService.encryptToBase64("credential",
        Encryption.USER_CREDENTIAL_ENCRYPTION_ASSOCIATED_DATA.getBytes());
    Principal userWithCredEncrypted = new Principal("userFoo", Principal.PrincipalType.USER, null,
        new Credential(cred, Credential.CredentialType.EXTERNAL_ENCRYPTED));

    ControllerWrapper controllerWrapper = createControllerWrapper(CCONF, sConfCopy, null);
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer accessEnforcer = controllerWrapper.defaultAccessEnforcer;

    assertAuthorizationFailure(accessEnforcer, NS, userWithCredEncrypted,
        StandardPermission.UPDATE);

    accessController.grant(Authorizable.fromEntityId(NS), userWithCredEncrypted,
        ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE));

    accessEnforcer.enforce(NS, userWithCredEncrypted, StandardPermission.GET);
    accessEnforcer.enforce(NS, userWithCredEncrypted, StandardPermission.UPDATE);
    // Verify the metrics context was called with correct metrics
    verify(controllerWrapper.mockMetricsContext, times(2))
        .increment(Constants.Metrics.Authorization.EXTENSION_CHECK_SUCCESS_COUNT, 1);
    verify(controllerWrapper.mockMetricsContext, times(1))
        .increment(Constants.Metrics.Authorization.EXTENSION_CHECK_FAILURE_COUNT, 1);
    verify(controllerWrapper.mockMetricsContext, times(3))
        .gauge(eq(Constants.Metrics.Authorization.EXTENSION_CHECK_MILLIS), any(Long.class));
  }

  @Test
  public void testAuthEnforceWithBadEncryptedCredential()
      throws AccessException {
    thrown.expect(Exception.class);
    thrown.expectMessage("Failed to decrypt credential in principle:");

    SConfiguration sConfCopy = enableCredentialEncryption();
    String badCipherCred = Base64.getEncoder()
        .encodeToString("invalid encrypted credential".getBytes());

    Principal userWithCredEncrypted = new Principal("userFoo", Principal.PrincipalType.USER, null,
        new Credential(badCipherCred,
            Credential.CredentialType.EXTERNAL_ENCRYPTED));

    ControllerWrapper controllerWrapper = createControllerWrapper(CCONF, sConfCopy, null);
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer accessEnforcer = controllerWrapper.defaultAccessEnforcer;

    accessController.grant(Authorizable.fromEntityId(NS), userWithCredEncrypted,
        ImmutableSet.of(StandardPermission.GET, StandardPermission.GET));

    accessEnforcer.enforce(NS, userWithCredEncrypted, StandardPermission.GET);
    // Verify the metrics context was not called
    verify(controllerWrapper.mockMetricsContext, times(0))
        .increment(any(String.class), any(Long.class));
    verify(controllerWrapper.mockMetricsContext, times(0))
        .gauge(any(String.class), any(Long.class));
  }

  @Test
  public void testIsVisibleWithEncryptedCredential() throws AccessException, CipherException {
    SConfiguration sConfCopy = enableCredentialEncryption();
    String cred = fakeAeadCipherService.encryptToBase64("credential",
        Encryption.USER_CREDENTIAL_ENCRYPTION_ASSOCIATED_DATA.getBytes());
    Principal userWithCredEncrypted = new Principal("userFoo", Principal.PrincipalType.USER, null,
        new Credential(cred, Credential.CredentialType.EXTERNAL_ENCRYPTED));

    ControllerWrapper controllerWrapper = createControllerWrapper(CCONF, sConfCopy, null);
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer accessEnforcer = controllerWrapper.defaultAccessEnforcer;

    Set<NamespaceId> namespaces = ImmutableSet.of(NS);

    Assert.assertEquals(0, accessEnforcer.isVisible(namespaces, userWithCredEncrypted).size());

    accessController.grant(Authorizable.fromEntityId(NS), userWithCredEncrypted,
        ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE));

    Assert.assertEquals(1, accessEnforcer.isVisible(namespaces, userWithCredEncrypted).size());
    // Verify the metrics context was called with correct metrics
    verify(controllerWrapper.mockMetricsContext, times(2))
        .increment(Constants.Metrics.Authorization.NON_INTERNAL_VISIBILITY_CHECK_COUNT, 1);
    verify(controllerWrapper.mockMetricsContext, times(2))
        .gauge(eq(Constants.Metrics.Authorization.EXTENSION_VISIBILITY_MILLIS), any(Long.class));
  }

  @Test
  public void testSystemUser() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    Principal systemUser =
        new Principal(UserGroupInformation.getCurrentUser().getShortUserName(),
            Principal.PrincipalType.USER);
    ControllerWrapper controllerWrapper = createControllerWrapper(cConfCopy, SCONF, null);
    DefaultAccessEnforcer accessEnforcer = controllerWrapper.defaultAccessEnforcer;
    NamespaceId ns1 = new NamespaceId("ns1");
    accessEnforcer.enforce(NamespaceId.SYSTEM, systemUser, EnumSet.allOf(StandardPermission.class));
    accessEnforcer.enforce(NamespaceId.SYSTEM, systemUser, StandardPermission.GET);
    Assert.assertEquals(ImmutableSet.of(NamespaceId.SYSTEM),
        accessEnforcer.isVisible(ImmutableSet.of(ns1, NamespaceId.SYSTEM),
            systemUser));
    // Verify the metrics context was called with correct metrics
    verify(controllerWrapper.mockMetricsContext, times(2))
        .increment(Constants.Metrics.Authorization.EXTENSION_CHECK_BYPASS_COUNT, 1);
  }

  @Test
  public void testInternalAuthEnforce() throws IOException, AccessException {
    Principal userWithInternalCred = new Principal("system", Principal.PrincipalType.USER, null,
        new Credential("credential",
            Credential.CredentialType.INTERNAL));
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    ControllerWrapper controllerWrapper = createControllerWrapper(cConfCopy, SCONF,
        new NoOpAccessController());
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer accessEnforcer = controllerWrapper.defaultAccessEnforcer;
    // Make sure that the actual access controller does not have access.
    assertAuthorizationFailure(accessController, NS, userWithInternalCred, StandardPermission.GET);
    assertAuthorizationFailure(accessController, NS, userWithInternalCred,
        StandardPermission.UPDATE);
    // The no-op access enforcer allows all requests through, so this should succeed if it is using the right
    // access controller.
    accessEnforcer.enforce(NS, userWithInternalCred, StandardPermission.GET);
    accessEnforcer.enforce(NS, userWithInternalCred, StandardPermission.UPDATE);
    // Verify the metrics context was called with correct metrics
    verify(controllerWrapper.mockMetricsContext, times(2))
        .increment(Constants.Metrics.Authorization.INTERNAL_CHECK_SUCCESS_COUNT, 1);
  }

  @Test
  public void testInternalIsVisible() throws AccessException {
    Principal userWithInternalCred = new Principal("system", Principal.PrincipalType.USER, null,
        new Credential("credential",
            Credential.CredentialType.INTERNAL));
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    ControllerWrapper controllerWrapper = createControllerWrapper(cConfCopy, SCONF,
        new NoOpAccessController());
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer accessEnforcer = controllerWrapper.defaultAccessEnforcer;
    Set<EntityId> namespaces = ImmutableSet.of(NS);
    // Make sure that the actual access controller does not have access.
    Assert.assertEquals(Collections.emptySet(),
        accessController.isVisible(namespaces, userWithInternalCred));
    // The no-op access enforcer allows all requests through, so this should succeed if it is using the right
    // access controller.
    Assert.assertEquals(namespaces, accessEnforcer.isVisible(namespaces, userWithInternalCred));
    // Verify the metrics context was called with correct metrics
    verify(controllerWrapper.mockMetricsContext, times(1))
        .increment(Constants.Metrics.Authorization.INTERNAL_VISIBILITY_CHECK_COUNT, 1);
  }

  @Test
  public void testMetricsContextNotCalledIfDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Metrics.AUTHORIZATION_METRICS_ENABLED, false);
    ControllerWrapper controllerWrapper = createControllerWrapper(cConfCopy, SCONF, null);
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer accessEnforcer = controllerWrapper.defaultAccessEnforcer;
    DatasetId ds = NS.dataset("ds");
    accessController
        .grant(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(StandardPermission.GET,
            StandardPermission.UPDATE));
    accessEnforcer
        .enforce(NS, ALICE, ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE));
    // Verify the metrics context was not called
    verify(controllerWrapper.mockMetricsContext, times(0))
        .increment(any(String.class), any(Long.class));
    verify(controllerWrapper.mockMetricsContext, times(0))
        .gauge(any(String.class), any(Long.class));
  }

  private SConfiguration enableCredentialEncryption() {
    SConfiguration sConfCopy = SConfiguration.copy(SCONF);
    sConfCopy.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED, "true");
    return sConfCopy;
  }

  private void verifyDisabled(CConfiguration cConf) throws IOException, AccessException {
    ControllerWrapper controllerWrapper = createControllerWrapper(cConf, SCONF, null);
    AccessController accessController = controllerWrapper.accessController;
    DefaultAccessEnforcer authEnforcementService = controllerWrapper.defaultAccessEnforcer;
    DatasetId ds = NS.dataset("ds");
    // All enforcement operations should succeed, since authorization is disabled
    accessController.grant(Authorizable.fromEntityId(ds), BOB,
        ImmutableSet.of(StandardPermission.UPDATE));
    authEnforcementService.enforce(NS, ALICE, StandardPermission.UPDATE);
    authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
    authEnforcementService.enforce(NS, BOB, StandardPermission.GET);
    authEnforcementService.enforce(ds, BOB, StandardPermission.GET);
    Assert.assertEquals(2,
        authEnforcementService.isVisible(ImmutableSet.<EntityId>of(NS, ds), BOB).size());
    // Verify the metrics context was not called
    verify(controllerWrapper.mockMetricsContext, times(0))
        .increment(any(String.class), any(Long.class));
    verify(controllerWrapper.mockMetricsContext, times(0))
        .gauge(any(String.class), any(Long.class));
  }

  private void assertAuthorizationFailure(AccessEnforcer authEnforcementService,
      EntityId entityId, Principal principal,
      Permission permission) throws AccessException {
    try {
      authEnforcementService.enforce(entityId, principal, permission);
      Assert.fail(String.format("Expected %s to not have '%s' privilege on %s but it does.",
          principal, permission, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private void assertAuthorizationFailure(AccessEnforcer authEnforcementService,
      EntityType entityType,
      EntityId parentId, Principal principal,
      Permission permission) throws AccessException {
    try {
      authEnforcementService.enforceOnParent(entityType, parentId, principal, permission);
      Assert.fail(String.format("Expected %s to not have '%s' privilege on %s in %s but it does.",
          principal, permission, entityType, parentId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private void assertAuthorizationFailure(AccessEnforcer authEnforcementService,
      EntityId entityId, Principal principal,
      Set<? extends Permission> permissions) throws AccessException {
    try {
      authEnforcementService.enforce(entityId, principal, permissions);
      Assert.fail(String.format("Expected %s to not have '%s' privileges on %s but it does.",
          principal, permissions, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  @Test
  public void testExpectedMetricsTagsForEntityId() {
    String namespaceName = "namespace";
    NamespaceId namespaceId = new NamespaceId(namespaceName);
    Map<String, String> expectedTags = new HashMap<>();
    expectedTags.put(Constants.Metrics.Tag.NAMESPACE, namespaceName);
    Map<String, String> tags = DefaultAccessEnforcer.createEntityIdMetricsTags(namespaceId);
    Assert.assertEquals(expectedTags, tags);
  }

  @Test
  public void testExpectedMetricsTagsForChildEntityId() {
    String namespaceName = "namespace";
    NamespaceId namespaceId = new NamespaceId(namespaceName);
    String appName = "app";
    ApplicationId applicationId = namespaceId.app(appName);
    String programName = "program";
    ProgramId programId = applicationId.program(ProgramType.SPARK, programName);
    Map<String, String> expectedTags = new HashMap<>();
    expectedTags.put(Constants.Metrics.Tag.NAMESPACE, namespaceName);
    expectedTags.put(Constants.Metrics.Tag.APP, appName);
    expectedTags.put(Constants.Metrics.Tag.PROGRAM, programName);
    expectedTags.put(Constants.Metrics.Tag.PROGRAM_TYPE,
        ProgramTypeMetricTag.getTagName(programId.getType()));
    Map<String, String> tags = DefaultAccessEnforcer.createEntityIdMetricsTags(programId);
    Assert.assertEquals(expectedTags, tags);
  }
}
