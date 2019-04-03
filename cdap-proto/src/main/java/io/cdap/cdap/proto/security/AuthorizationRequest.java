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

package co.cask.cdap.proto.security;

import co.cask.cdap.api.annotation.Beta;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Contains components for an Authorization request.
 */
@Beta
public class AuthorizationRequest {

  private final Authorizable authorizable;
  private final Principal principal;
  private final Set<Action> actions;

  protected AuthorizationRequest(Authorizable authorizable, @Nullable Principal principal,
                                 @Nullable Set<Action> actions) {
    if (authorizable == null) {
      throw new IllegalArgumentException("Authorizable is required");
    }
    this.authorizable = authorizable;
    this.principal = principal;
    this.actions = (actions != null) ? Collections.unmodifiableSet(new LinkedHashSet<>(actions)) : null;
  }

  public Authorizable getAuthorizable() {
    return authorizable;
  }

  @Nullable
  public Principal getPrincipal() {
    return principal;
  }

  @Nullable
  public Set<Action> getActions() {
    return actions;
  }
}
