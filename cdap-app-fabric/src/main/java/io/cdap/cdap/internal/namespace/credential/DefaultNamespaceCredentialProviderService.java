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

package io.cdap.cdap.internal.namespace.credential;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.internal.credential.CredentialProfileManager;
import io.cdap.cdap.internal.credential.CredentialProviderService;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.credential.CredentialProvisionContext;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.NamespacePermission;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Default implementation for {@link NamespaceCredentialProviderService} used in AppFabric.
 */
public class DefaultNamespaceCredentialProviderService extends AbstractIdleService
    implements NamespaceCredentialProviderService {

  public static final String SYSTEM_P4SA_IDENTITY_NAME = "system-p4sa";
  private static final String CREDENTIAL_PROVIDER_NAME = "gcp-wi-credential-provider";
  private static final String GOOGLE_MANAGED_SA_SUBSTRING = "@gcp-sa-";
  private static final String TENANT_PROJECT_SA_SUFFIX = "-tp.iam.gserviceaccount.com";

  private final CredentialProviderService credentialProvider;
  private final NamespaceAdmin namespaceAdmin;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private final CredentialProfileManager credentialProfileManager;
  private final CredentialIdentityManager credentialIdentityManager;
  private final CConfiguration cConf;
  private final FeatureFlagsProvider featureFlagsProvider;

  /**
   * Constructs {@link DefaultNamespaceCredentialProviderService}.
   */
  @Inject
  public DefaultNamespaceCredentialProviderService(CConfiguration cConf,
      CredentialProviderService credentialProvider, ContextAccessEnforcer contextAccessEnforcer,
      NamespaceAdmin namespaceAdmin, CredentialProfileManager credentialProfileManager,
      CredentialIdentityManager credentialIdentityManager) {
    this.cConf = cConf;
    this.credentialProvider = credentialProvider;
    this.contextAccessEnforcer = contextAccessEnforcer;
    this.namespaceAdmin = namespaceAdmin;
    this.credentialProfileManager = credentialProfileManager;
    this.credentialIdentityManager = credentialIdentityManager;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
  }

  /**
   * Provisions a short-lived credential for the provided identity using the provided identity.
   *
   * @param namespace The identity namespace.
   * @param scopes    A comma separated list of OAuth scopes requested.
   * @return A short-lived credential.
   * @throws CredentialProvisioningException If provisioning the credential fails.
   * @throws IOException                     If any transport errors occur.
   * @throws NotFoundException               If the profile or identity are not found.
   */
  @Override
  public ProvisionedCredential provision(String namespace, String scopes)
      throws CredentialProvisioningException, IOException, NotFoundException {
    contextAccessEnforcer.enforce(new NamespaceId(namespace),
        NamespacePermission.PROVISION_CREDENTIAL);
    NamespaceMeta namespaceMeta;
    NamespaceId namespaceId = new NamespaceId(namespace);
    try {
      namespaceMeta = namespaceAdmin.get(namespaceId);
    } catch (Exception e) {
      throw new IOException(String.format("Failed to get namespace '%s' metadata",
          namespace), e);
    }
    String identityName =
        GcpWorkloadIdentityUtil.getWorkloadIdentityName(namespaceId);
    switchToInternalUser();
    return credentialProvider.provision(NamespaceId.SYSTEM.getNamespace(), identityName,
        new CredentialProvisionContext(
            GcpWorkloadIdentityUtil.createProvisionPropertiesMap(scopes, namespaceMeta)));
  }

  @Override
  public ProvisionedCredential provisionSystemP4sa(@Nullable String scopes)
      throws CredentialProvisioningException, IOException, NotFoundException {
    switchToInternalUser();
    return credentialProvider.provisionSystemP4sa(
        new CredentialProvisionContext(
            GcpWorkloadIdentityUtil.createProvisionPropertiesMap(scopes, NamespaceMeta.DEFAULT)));
  }

  @Override
  public void validateIdentity(String namespace, String serviceAccount)
      throws IdentityValidationException, IOException {
    if (Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.isEnabled(featureFlagsProvider)) {
      validateAllowedServiceAccount(serviceAccount);
    }
    NamespaceMeta namespaceMeta;
    try {
      namespaceMeta = namespaceAdmin.get(new NamespaceId(namespace));
    } catch (Exception e) {
      throw new IOException(String.format("Failed to get namespace '%s' metadata",
          namespace), e);
    }
    CredentialIdentity credentialIdentity = new CredentialIdentity(
        NamespaceId.SYSTEM.getNamespace(), GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME,
        namespaceMeta.getIdentity(), serviceAccount);
    try {
      credentialProvider
          .validateIdentity(NamespaceId.SYSTEM.getNamespace(), credentialIdentity,
              new CredentialProvisionContext(
                  GcpWorkloadIdentityUtil.createProvisionPropertiesMap(null, namespaceMeta)));
    } catch (NotFoundException e) {
      // This should never happen.
      throw new IdentityValidationException("Could not find GCP workload identity provider profile",
          e);
    }
  }

  private void validateAllowedServiceAccount(@Nullable String serviceAccount)
      throws IdentityValidationException {
    String normalizedSa = Strings.nullToEmpty(serviceAccount).trim().toLowerCase(Locale.ROOT);
    if (normalizedSa.isEmpty()) {
      throw new IdentityValidationException("Namespace Service Account email must not be empty.");
    }
    String configuredSystemP4sa =
        Strings.nullToEmpty(cConf.get(Constants.CredentialProvider.SYSTEM_P4SA_SERVICE_ACCOUNT, ""))
            .trim()
            .toLowerCase(Locale.ROOT);
    if (normalizedSa.contains(GOOGLE_MANAGED_SA_SUBSTRING)
        || normalizedSa.endsWith(TENANT_PROJECT_SA_SUFFIX)
        || (!configuredSystemP4sa.isEmpty() && normalizedSa.equals(configuredSystemP4sa))) {
      throw new IdentityValidationException(
          String.format(
              "Service account '%s' is a Google-managed or system service account and cannot be "
                  + "attached as a Namespace Service Account.",
              serviceAccount));
    }
  }

  private void switchToInternalUser() {
    SecurityRequestContext.resetUserDetails();
  }

  /**
   * Start the service.
   */
  @Override
  protected void startUp() throws Exception {
    // Updates the namespaces and creates the identities if not exist.
    if (Feature.NAMESPACED_SERVICE_ACCOUNTS.isEnabled(featureFlagsProvider)
        || Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.isEnabled(featureFlagsProvider)) {
      List<NamespaceMeta> namespaceMetaList = namespaceAdmin.list();
      for (NamespaceMeta namespaceMeta : namespaceMetaList) {
        if (Strings.isNullOrEmpty(namespaceMeta.getIdentity())) {
          String identityName = namespaceAdmin.getIdentity(namespaceMeta.getNamespaceId());
          MasterEnvironment masterEnv = MasterEnvironments.getMasterEnvironment();
          if (masterEnv != null
              && !cConf.getBoolean(Constants.Namespace.NAMESPACE_CREATION_HOOK_ENABLED)) {
            masterEnv.createIdentity(NamespaceId.DEFAULT.getNamespace(), identityName);
          }
          NamespaceMeta newNamespaceMeta =
              new NamespaceMeta.Builder(namespaceMeta)
                  .setIdentity(identityName)
                  .build();
          namespaceAdmin.updateProperties(newNamespaceMeta.getNamespaceId(), newNamespaceMeta);
        }
      }
    }
    // create the system profile if not exists.
    createSystemProfileIfNotExists();
    upsertSystemP4saIdentityIfConfigured();
  }

  private void createSystemProfileIfNotExists() throws BadRequestException, IOException {
    CredentialProfileId profileId = new CredentialProfileId(NamespaceId.SYSTEM.getNamespace(),
        GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME);
    CredentialProfile profile = new CredentialProfile(
        CREDENTIAL_PROVIDER_NAME,
        "System Credential Profile for GCP Workload Identity Credential Provider",
        Collections.emptyMap());
    try {
      credentialProfileManager.create(profileId, profile);
    } catch (AlreadyExistsException e) {
      // ignore if the profile already exists.
    }
  }

  private void upsertSystemP4saIdentityIfConfigured() throws Exception {
    String systemP4sa =
        Strings.nullToEmpty(cConf.get(Constants.CredentialProvider.SYSTEM_P4SA_SERVICE_ACCOUNT, ""))
            .trim();
    String systemK8sIdentity =
        Strings.nullToEmpty(cConf.get(Constants.CredentialProvider.SYSTEM_P4SA_K8S_IDENTITY, ""))
            .trim();
    if (systemP4sa.isEmpty() || systemK8sIdentity.isEmpty()) {
      return;
    }

    CredentialIdentityId identityId = new CredentialIdentityId(
        NamespaceId.SYSTEM.getNamespace(), SYSTEM_P4SA_IDENTITY_NAME);
    CredentialIdentity credentialIdentity = new CredentialIdentity(
        NamespaceId.SYSTEM.getNamespace(), GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME,
        systemK8sIdentity, systemP4sa);

    Retries.runWithRetries(() -> {
      switchToInternalUser();
      try {
        credentialIdentityManager.create(identityId, credentialIdentity);
      } catch (AlreadyExistsException e) {
        credentialIdentityManager.update(identityId, credentialIdentity);
      }
    }, RetryStrategies.limit(3, RetryStrategies.fixDelay(500, TimeUnit.MILLISECONDS)),
        t -> t instanceof IOException);
  }

  /**
   * Stop the service.
   */
  @Override
  protected void shutDown() throws Exception {

  }
}
