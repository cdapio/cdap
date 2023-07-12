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

package io.cdap.cdap.internal.credential;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.credential.CredentialProvisioningException;
import io.cdap.cdap.api.security.credential.ProvisionedCredential;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.internal.credential.store.CredentialIdentityStore;
import io.cdap.cdap.internal.credential.store.CredentialProfileStore;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.cdap.security.spi.credential.CredentialProvider;
import io.cdap.cdap.security.spi.credential.ProfileValidationException;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition.CredentialProviderStore;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class CredentialProviderTestBase {

  private static TransactionManager txManager;
  static ContextAccessEnforcer contextAccessEnforcer;
  static CredentialProfileManager credentialProfileManager;
  static CredentialIdentityManager credentialIdentityManager;
  static Map<String, CredentialProvider> credentialProviders = new HashMap<>();

  // Some pre-created mock credential providers which succeed, fail provisioning, or fail validation.
  static String CREDENTIAL_PROVIDER_TYPE_SUCCESS = "success";
  static String CREDENTIAL_PROVIDER_TYPE_VALIDATION_FAILURE = "validationFailure";
  static String CREDENTIAL_PROVIDER_TYPE_PROVISION_FAILURE = "provisionFailure";

  static ProvisionedCredential RETURNED_TOKEN = new ProvisionedCredential("returned_token", 9999);

  static class MockCredentialProviderProvider implements CredentialProviderProvider {

    @Override
    public Map<String, CredentialProvider> loadCredentialProviders() {
      return credentialProviders;
    }
  }

  @BeforeClass
  public static void beforeClass() throws CredentialProvisioningException, IOException,
      ProfileValidationException {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new ConfigModule(cConf),
        new SystemDatasetRuntimeModule().getInMemoryModules(),
        new StorageModule(),
        new TransactionModules().getInMemoryModules(),
        new AuthorizationEnforcementModule().getNoOpModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
                .in(Scopes.SINGLETON);
          }
        });
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    contextAccessEnforcer = injector.getInstance(ContextAccessEnforcer.class);
    CredentialProviderStore.create(injector
        .getInstance(StructuredTableAdmin.class));
    // Setup mock credential providers.
    CredentialProvider mockCredentialProvider = mock(CredentialProvider.class);
    when(mockCredentialProvider.provision(any(), any())).thenReturn(RETURNED_TOKEN);
    CredentialProvider validationFailureMockCredentialProvider = mock(CredentialProvider.class);
    when(validationFailureMockCredentialProvider.provision(any(), any()))
        .thenReturn(RETURNED_TOKEN);
    doThrow(new ProfileValidationException("profile validation always fails with this provider"))
        .when(validationFailureMockCredentialProvider).validateProfile(any());
    CredentialProvider provisionFailureMockCredentialProvider = mock(CredentialProvider.class);
    when(provisionFailureMockCredentialProvider.provision(any(), any()))
        .thenThrow(new CredentialProvisioningException("provisioning always fails with this "
            + "provider"));
    credentialProviders.put(CREDENTIAL_PROVIDER_TYPE_SUCCESS, mockCredentialProvider);
    credentialProviders.put(CREDENTIAL_PROVIDER_TYPE_VALIDATION_FAILURE,
        validationFailureMockCredentialProvider);
    credentialProviders.put(CREDENTIAL_PROVIDER_TYPE_PROVISION_FAILURE,
        provisionFailureMockCredentialProvider);
    CredentialProviderProvider mockCredentialProviderProvider
        = new MockCredentialProviderProvider();

    // Setup credential managers.
    TransactionRunner runner = injector.getInstance(TransactionRunner.class);
    CredentialProfileStore profileStore = new CredentialProfileStore();
    CredentialIdentityStore identityStore = new CredentialIdentityStore();
    credentialProfileManager = new CredentialProfileManager(identityStore, profileStore,
        runner, mockCredentialProviderProvider);
    credentialIdentityManager = new CredentialIdentityManager(identityStore, profileStore,
        runner);
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }

  CredentialProfileId createDummyProfile(String type, String namespace, String name)
      throws Exception {
    CredentialProfile profile = new CredentialProfile(type, "some description",
        Collections.singletonMap("some-key", "some-value"));
    CredentialProfileId profileId = new CredentialProfileId(namespace, name);
    credentialProfileManager.create(profileId, profile);
    return profileId;
  }
}
