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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.security.ACL;
import co.cask.cdap.api.security.EntityId;
import co.cask.cdap.api.security.PermissionType;
import co.cask.cdap.api.security.Principal;
import co.cask.cdap.api.security.PrincipalType;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

/**
 * In-memory implementation of {@link ACLClient}, for testing.
 */
public class InMemoryACLClient extends ACLClient {

  private Multimap<EntityId, ACL> acls;

  public InMemoryACLClient() {
    super(null);
    this.acls = HashMultimap.create();
  }

  @Override
  public List<ACL> listAcls(EntityId entityId) {
    return ImmutableList.copyOf(acls.get(entityId));
  }

  @Override
  public List<ACL> listAcls(EntityId entityId, String userId) {
    final String targetUserId = userId;
    return ImmutableList.copyOf(Iterables.filter(acls.get(entityId), new Predicate<ACL>() {
      @Override
      public boolean apply(@Nullable ACL input) {
        return input != null && input.getPrincipal().equals(new Principal(PrincipalType.USER, targetUserId));
      }
    }));
  }

  @Override
  public void setAclForUser(EntityId entityId, String userId, List<PermissionType> permissions) {
    setACLForPrincipal(acls.get(entityId), new Principal(PrincipalType.USER, userId), permissions);
  }

  @Override
  public void setAclForGroup(EntityId entityId, String groupId, List<PermissionType> permissions) {
    setACLForPrincipal(acls.get(entityId), new Principal(PrincipalType.GROUP, groupId), permissions);
  }

  private void setACLForPrincipal(Collection<ACL> acls, final Principal principal,
                                  Iterable<PermissionType> permissions) {
    Iterables.removeIf(acls, new Predicate<ACL>() {
      @Override
      public boolean apply(@Nullable ACL acl) {
        return acl != null && principal.equals(acl.getPrincipal());
      }
    });

    ACL acl = new ACL(principal, permissions);
    acls.add(acl);
  }

}
