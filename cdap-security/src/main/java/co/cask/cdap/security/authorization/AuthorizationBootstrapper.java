/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

/**
 * A class to bootstrap authorization
 */
public class AuthorizationBootstrapper {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizationBootstrapper.class);

  private final boolean enabled;
  private final PrivilegesManager privilegesManager;
  private final Principal systemUser;
  private final Set<Principal> adminUsers;
  private final InstanceId instanceId;

  @Inject
  AuthorizationBootstrapper(CConfiguration cConf, PrivilegesManager privilegesManager) {
    this.enabled =
      cConf.getBoolean(Constants.Security.ENABLED) && cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    String currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    this.systemUser = new Principal(currentUser, Principal.PrincipalType.USER);
    this.adminUsers = getAdminUsers(cConf);
    if (enabled && adminUsers.isEmpty()) {
      LOG.info("Admin users specified by {} is empty.", Constants.Security.Authorization.ADMIN_USERS);
    }
    this.instanceId = new InstanceId(cConf.get(Constants.INSTANCE_NAME));
    this.privilegesManager = privilegesManager;
  }

  public void run() {
    if (!enabled) {
      return;
    }
    LOG.debug("Bootstrapping authorization for CDAP instance: {}, system users: {} and admin users: {}",
              instanceId, systemUser, adminUsers);
    try {
      // grant admin on instance, so the system user can create default (and other) namespaces
      privilegesManager.grant(instanceId, systemUser, Collections.singleton(Action.ADMIN));
      // grant ALL on the system namespace, so the system user can create and access tables in the system namespace
      // also required by SystemArtifactsLoader to add system artifacts
      privilegesManager.grant(NamespaceId.SYSTEM, systemUser, EnumSet.allOf(Action.class));
      for (Principal adminUser : adminUsers) {
        // grant admin privileges on the CDAP instance to the admin users, so they can create namespaces
        privilegesManager.grant(instanceId, adminUser, Collections.singleton(Action.ADMIN));
        // also grant admin on the default namespace, so admins can also manage privileges on them
        privilegesManager.grant(NamespaceId.DEFAULT, adminUser, Collections.singleton(Action.ADMIN));
      }
      LOG.info("Successfully bootstrapped authorization for CDAP instance {}, system user {} and admin users: {}",
               instanceId, systemUser, adminUsers);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Set<Principal> getAdminUsers(CConfiguration cConf) {
    Set<Principal> admins = new HashSet<>();
    String adminUsers = cConf.get(Constants.Security.Authorization.ADMIN_USERS);
    if (adminUsers != null) {
      for (String adminUser : Splitter.on(",").omitEmptyStrings().trimResults().split(adminUsers)) {
        admins.add(new Principal(adminUser, Principal.PrincipalType.USER));
      }
    }
    return admins;
  }
}
