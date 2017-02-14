/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.kerberos;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.FeatureDisabledException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.inject.Inject;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link OwnerAdmin};
 */
public class DefaultOwnerAdmin implements OwnerAdmin {

  private final CConfiguration cConf;
  private final OwnerStore ownerStore;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public DefaultOwnerAdmin(CConfiguration cConf, OwnerStore ownerStore, NamespaceQueryAdmin namespaceQueryAdmin) {
    this.cConf = cConf;
    this.ownerStore = ownerStore;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  public void add(NamespacedEntityId entityId, KerberosPrincipalId kerberosPrincipalId)
    throws IOException, AlreadyExistsException {
    ownerStore.add(entityId, kerberosPrincipalId);
  }

  @Nullable
  @Override
  public KerberosPrincipalId getOwner(NamespacedEntityId entityId) throws IOException {
    return ownerStore.getOwner(entityId);
  }

  @Nullable
  @Override
  public String getOwnerPrincipal(NamespacedEntityId entityId) throws IOException {
    KerberosPrincipalId owner = getOwner(entityId);
    return owner == null ? null : owner.getPrincipal();
  }

  @Nullable
  @Override
  public ImpersonationInfo getImpersonationInfo(NamespacedEntityId entityId,
                                                ImpersonatedOpType impersonatedOpType) throws IOException {
    // For program we look for application owner. In future we might want to support lookup parent owner
    // recursively once we have a use-case for it.
    if (entityId.getEntityType().equals(EntityType.PROGRAM)) {
      entityId = ((ProgramId) entityId).getParent();
    }
    if (!entityId.getEntityType().equals(EntityType.NAMESPACE)) {
      KerberosPrincipalId effectiveOwner = ownerStore.getOwner(entityId);
      if (effectiveOwner != null) {
        return new ImpersonationInfo(effectiveOwner.getPrincipal(),
                                     SecurityUtil.getKeytabURIforPrincipal(effectiveOwner.getPrincipal(), cConf));
      }
    }
    // (CDAP-8176) Since no owner was found for the entity return namespace principal if present.
    try {
      NamespaceConfig nsConfig = namespaceQueryAdmin.get(entityId.getNamespaceId()).getConfig();
      if (impersonatedOpType.equals(ImpersonatedOpType.EXPLORE) && !nsConfig.isExploreAsPrincipal()) {
        throw new FeatureDisabledException(FeatureDisabledException.Feature.EXPLORE,
                                           NamespaceConfig.class.getSimpleName() + " of " + entityId,
                                           NamespaceConfig.EXPLORE_AS_PRINCIPAL, String.valueOf(true));
      }
      String nsPrincipal = nsConfig.getPrincipal();
      return nsPrincipal == null ? null : new ImpersonationInfo(nsPrincipal, nsConfig.getKeytabURI());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean exists(NamespacedEntityId entityId) throws IOException {
    return ownerStore.exists(entityId);
  }

  @Override
  public void delete(NamespacedEntityId entityId) throws IOException {
    ownerStore.delete(entityId);
  }
}
