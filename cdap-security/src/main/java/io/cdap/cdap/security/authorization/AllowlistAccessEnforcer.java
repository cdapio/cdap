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

package io.cdap.cdap.security.authorization;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the RemoteAccessEnforcer. It is used to skip authorization enforcement for
 * some users.
 */
public class AllowlistAccessEnforcer extends AbstractAccessEnforcer {

  private static final Logger LOG = LoggerFactory.getLogger(AllowlistAccessEnforcer.class);
  public static final String ALLOWLIST_DELEGATE_ACCESS_ENFORCER =
      "allowlist.delegate.access.enforcer";
  public static final String ACCESS_ENFORCER_ALLOWLIST_USERS =
      "security.authorization.access.enforcer.allowlist.users";
  private final AccessEnforcer delegate;
  private final List<String> allowlistUsers;

  @Inject
  AllowlistAccessEnforcer(
      @Named(ALLOWLIST_DELEGATE_ACCESS_ENFORCER) AccessEnforcer accessEnforcer,
      CConfiguration cConf) {
    super(cConf);
    delegate = accessEnforcer;
    allowlistUsers = Arrays.asList(cConf.get(ACCESS_ENFORCER_ALLOWLIST_USERS).split(","));
  }

  @Override
  public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
      throws AccessException {
    if (principal != null && allowlistUsers.contains(principal.getName())) {
      // skip authorization enforcement when user is yarn.
      LOG.debug("Skipping authorization enforcement for user '{}'", principal.getName());
      return;
    }
    delegate.enforce(entity, principal, permissions);
  }

  @Override
  public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal,
      Permission permission) throws AccessException {
    if (principal != null && allowlistUsers.contains(principal.getName())) {
      // skip authorization enforcement when user is yarn.
      LOG.debug("Skipping authorization enforcement for user '{}'", principal.getName());
      return;
    }
    delegate.enforceOnParent(entityType, parentId, principal, permission);
  }

  @Override
  public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal)
      throws AccessException {
    return delegate.isVisible(entityIds, principal);
  }
}
