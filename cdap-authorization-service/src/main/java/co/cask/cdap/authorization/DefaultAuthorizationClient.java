/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.authorization;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.SecurityRequestContext;
import co.cask.common.authorization.ACLStore;
import co.cask.common.authorization.ObjectId;
import co.cask.common.authorization.Permission;
import co.cask.common.authorization.SubjectId;
import co.cask.common.authorization.client.ACLStoreSupplier;
import co.cask.common.authorization.client.AuthorizationClient;
import co.cask.common.authorization.client.SimpleAuthorizationClient;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

/**
 * Default implementation of {@link AuthorizationClient} which uses {@link ACLStore}.
 * Allows all operations when authorization is enabled or if the user is a configured administrator.
 */
public class DefaultAuthorizationClient extends SimpleAuthorizationClient {

  private final boolean authorizationEnabled;
  private final ImmutableSet<String> admins;

  /**
   * @param aclStoreSupplier used to set and get {@link co.cask.common.authorization.ACLEntry}s
   */
  @Inject
  public DefaultAuthorizationClient(ACLStoreSupplier aclStoreSupplier, CConfiguration configuration) {
    super(aclStoreSupplier);
    this.authorizationEnabled = configuration.getBoolean(Constants.Security.AUTHORIZATION_ENABLED);
    this.admins = ImmutableSet.copyOf(Splitter.on(",").split(
      Objects.firstNonNull(configuration.get(Constants.Security.ADMINS), "")));
  }

  public DefaultAuthorizationClient(ACLStore aclStore, CConfiguration configuration) {
    super(aclStore);
    this.authorizationEnabled = configuration.getBoolean(Constants.Security.AUTHORIZATION_ENABLED);
    this.admins = ImmutableSet.copyOf(Splitter.on(",").split(
      Objects.firstNonNull(configuration.get(Constants.Security.ADMINS), "")));
  }

  @Override
  public boolean isAuthorized(Iterable<ObjectId> objects, Iterable<SubjectId> subjects,
                              Iterable<Permission> requiredPermissions) {
    if (!authorizationEnabled) {
      return true;
    }

    Optional<String> userId = SecurityRequestContext.getUserId();
    if (!userId.isPresent()) {
      return false;
    }

    // Handle initial admins
    if (admins.contains(userId.get())) {
      return true;
    }

    return super.isAuthorized(objects, subjects, requiredPermissions);
  }
}
