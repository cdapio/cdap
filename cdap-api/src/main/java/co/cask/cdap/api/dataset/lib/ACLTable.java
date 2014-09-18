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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.api.security.Principal;

import java.util.List;

/**
 * A dataset that stores ACLs.
 */
@Beta
public interface ACLTable extends Dataset {


  /**
   * Sets an ACL on an entityId for a principal.
   *
   * @param principal the principal to set the ACL for
   * @param entityId the entityId to set the ACL on
   * @param permissions the list of permissions to allow the user access to entityId
   */
  public ACL setAcl(Principal principal, EntityId entityId, Iterable<PermissionType> permissions);

  /**
   * Sets an ACL on an entityId for a principal.
   *
   * @param principal the user to set the ACL for
   * @param entityId the entityId to set the ACL on
   * @param permissions the list of permissions to allow the user access to the entityId
   */
  public ACL setAcl(Principal principal, EntityId entityId, PermissionType... permissions);

  /**
   * Gets ACLs belonging to an entityId and a list of principals.
   *
   * @param entity the entityId whose ACLs we want
   * @param principals the principals whose ACLs we want
   * @return the list of ACLs of ACLs relevant to an entityId, for a user and its groups
   */
  public List<ACL> getAcls(EntityId entity, Iterable<Principal> principals);

  /**
   * Gets ACLs belonging to an entityId and a list of principals.
   *
   * @param entity the entityId whose ACLs we want
   * @param principals the principals whose ACLs we want
   * @return the list of ACLs of ACLs relevant to an entityId, for a user and its groups
   */
  public List<ACL> getAcls(EntityId entity, Principal... principals);

  /**
   * Gets ACLs belonging an entityId.
   *
   * @param entityId the entityId whose ACLs we want
   * @return the list of ACLs relevant to the entityId
   */
  public List<ACL> getAcls(EntityId entityId);
}
