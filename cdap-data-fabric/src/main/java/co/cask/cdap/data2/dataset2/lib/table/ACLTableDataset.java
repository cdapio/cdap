/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.ACLTable;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedObjectStore;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.api.security.Principal;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.util.List;

/**
 * Dataset that manages a table of ACLs.
 */
public class ACLTableDataset extends AbstractDataset implements ACLTable {

  private static final Gson GSON = new Gson();

  private final IndexedObjectStore<ACL> aclObjectStore;

  public ACLTableDataset(DatasetSpecification spec, @EmbeddedDataset("acls") IndexedObjectStore<ACL> aclObjectStore) {
    super(spec.getName(), aclObjectStore);
    this.aclObjectStore = aclObjectStore;
  }

  /**
   * Sets an ACL on an entityId for a principal.
   *
   * @param principal the principal to set the ACL for
   * @param entityId the entityId to set the ACL on
   * @param permissions the list of permissions to allow the user access to entityId
   */
  public ACL setAcl(Principal principal, EntityId entityId, Iterable<PermissionType> permissions) {
    ACL acl = new ACL(principal, permissions);
    setAcl(new Key(entityId.getQualifiedId(), principal.getQualifiedId()), acl);
    return acl;
  }

  /**
   * Sets an ACL on an entityId for a principal.
   *
   * @param principal the user to set the ACL for
   * @param entityId the entityId to set the ACL on
   * @param permissions the list of permissions to allow the user access to the entityId
   */
  public ACL setAcl(Principal principal, EntityId entityId, PermissionType... permissions) {
    return this.setAcl(principal, entityId, ImmutableList.copyOf(permissions));
  }

  /**
   * Gets ACLs belonging to an entityId and a list of principals.
   *
   * @param entity the entityId whose ACLs we want
   * @param principals the principals whose ACLs we want
   * @return the list of ACLs of ACLs relevant to an entityId, for a user and its groups
   */
  public List<ACL> getAcls(EntityId entity, Iterable<Principal> principals) {
    List<ACL> result = Lists.newArrayList();
    for (Principal principal : principals) {
      ACL acl = readUnderlying(entity, principal);
      if (acl != null) {
        result.add(acl);
      }
    }
    return result;
  }

  /**
   * Gets ACLs belonging to an entityId and a list of principals.
   *
   * @param entity the entityId whose ACLs we want
   * @param principals the principals whose ACLs we want
   * @return the list of ACLs of ACLs relevant to an entityId, for a user and its groups
   */
  public List<ACL> getAcls(EntityId entity, Principal... principals) {
    return this.getAcls(entity, ImmutableList.copyOf(principals));
  }

  /**
   * Gets ACLs belonging an entityId.
   *
   * @param entityId the entityId whose ACLs we want
   * @return the list of ACLs relevant to the entityId
   */
  public List<ACL> getAcls(EntityId entityId) {
    return readUnderlying(entityId);
  }

  private void setAcl(Key key, ACL acl) {
    aclObjectStore.write(Bytes.toBytes(GSON.toJson(key)), acl,
                         new byte[][] { Bytes.toBytes(key.getQualifiedEntityIdentifier()) });
  }

  private List<ACL> readUnderlying(EntityId entityId) {
    return aclObjectStore.readAllByIndex(Bytes.toBytes(entityId.getQualifiedId()));
  }

  private ACL readUnderlying(EntityId entityId, Principal principal) {
    Key key = new Key(entityId.getQualifiedId(), principal.getQualifiedId());
    return aclObjectStore.read(Bytes.toBytes(GSON.toJson(key)));
  }

  /**
   * Used as the key for the {@link ACLTableDataset#aclObjectStore}.
   */
  private static final class Key {
    private final String qualifiedEntityIdentifier;
    private final String qualifiedPrincipalIdentifier;

    private Key(String qualifiedEntityIdentifier, String qualifiedPrincipalIdentifier) {
      this.qualifiedEntityIdentifier = qualifiedEntityIdentifier;
      this.qualifiedPrincipalIdentifier = qualifiedPrincipalIdentifier;
    }

    /**
     * @return the qualifiedEntityIdentifier (i.e. resource) to be accessed
     */
    public String getQualifiedEntityIdentifier() {
      return qualifiedEntityIdentifier;
    }

    /**
     * @return the principal which is accessing the qualifiedEntityIdentifier
     */
    public String getQualifiedPrincipalIdentifier() {
      return qualifiedPrincipalIdentifier;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("Key{");
      sb.append("qualifiedEntityIdentifier='").append(qualifiedEntityIdentifier).append('\'');
      sb.append(", qualifiedPrincipalIdentifier='").append(qualifiedPrincipalIdentifier).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

}
