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
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.credential.CredentialProfileManager;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.credential.CredentialProvider;
import io.cdap.cdap.proto.credential.CredentialProvisionContext;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.IdentityValidationException;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.NamespacePermission;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Default implementation for {@link NamespaceCredentialProviderService} used in AppFabric.
 */
public class DefaultNamespaceCredentialProviderService extends AbstractIdleService
    implements NamespaceCredentialProviderService {

  private static final String CREDENTIAL_PROVIDER_NAME = "gcp-wi-credential-provider";
  private final CredentialProvider credentialProvider;
  private final NamespaceAdmin namespaceAdmin;
  private final ContextAccessEnforcer contextAccessEnforcer;
  private final CredentialProfileManager credentialProfileManager;
  private final CConfiguration cConf;


  @Inject
  DefaultNamespaceCredentialProviderService(CConfiguration cConf,
      CredentialProvider credentialProvider, ContextAccessEnforcer contextAccessEnforcer,
      NamespaceAdmin namespaceAdmin, CredentialProfileManager credentialProfileManager) {
    this.cConf = cConf;
    this.credentialProvider = credentialProvider;
    this.contextAccessEnforcer = contextAccessEnforcer;
    this.namespaceAdmin = namespaceAdmin;
    this.credentialProfileManager = credentialProfileManager;
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
  public void validateIdentity(String namespace, String serviceAccount)
      throws IdentityValidationException, IOException {
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

  private void switchToInternalUser() {
    SecurityRequestContext.reset();
  }

  /**
   * Start the service.
   */
  @Override
  protected void startUp() throws Exception {
    // Updates the namespaces and creates the identities if not exist.
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
    // create the system profile if not exists.
    createSystemProfileIfNotExists();
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

  /**
   * Stop the service.
   */
  @Override
  protected void shutDown() throws Exception {

  }
}
