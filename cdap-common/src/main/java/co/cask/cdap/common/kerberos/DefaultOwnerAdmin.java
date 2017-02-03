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
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
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

  private final OwnerStore ownerStore;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public DefaultOwnerAdmin(OwnerStore ownerStore, NamespaceQueryAdmin namespaceQueryAdmin) {
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
  public KerberosPrincipalId getEffectiveOwner(NamespacedEntityId entityId) throws IOException {
    // For program we look for application owner. In future we might want to support lookup parent owner
    // recursively once we have a use-case for it.
    if (entityId.getEntityType().equals(EntityType.PROGRAM)) {
      entityId = ((ProgramId) entityId).getParent();
    }
    KerberosPrincipalId effectiveOwner = ownerStore.getOwner(entityId);
    if (effectiveOwner != null) {
      return effectiveOwner;
    }
    // (CDAP-8176) Since no owner was found for the entity return namespace principal if present.
    String nsPrincipal;
    try {
      nsPrincipal = namespaceQueryAdmin.get(entityId.getNamespaceId()).getConfig().getPrincipal();
      return nsPrincipal == null ? null : new KerberosPrincipalId(nsPrincipal);
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
