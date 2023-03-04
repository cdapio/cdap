/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.proto.security;

import io.cdap.cdap.api.annotation.Beta;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Contains components for an Authorization request.
 */
@Beta
public class AuthorizationRequest {

  private final Authorizable authorizable;
  private final Principal principal;
  //This will only be set by GSon when deserializing a legacy request with actions
  private final Set<Action> actions;
  private final Set<? extends Permission> permissions;

  protected AuthorizationRequest(Authorizable authorizable, @Nullable Principal principal,
      @Nullable Set<? extends Permission> permissions) {
    if (authorizable == null) {
      throw new IllegalArgumentException("Authorizable is required");
    }
    this.authorizable = authorizable;
    this.principal = principal;
    this.actions = null;
    this.permissions = permissions;
  }

  public Authorizable getAuthorizable() {
    return authorizable;
  }

  @Nullable
  public Principal getPrincipal() {
    return principal;
  }

  @Nullable
  @Deprecated
  public Set<Action> getActions() {
    return actions;
  }

  @Nullable
  public Set<? extends Permission> getPermissions() {
    return permissions;
  }
}
