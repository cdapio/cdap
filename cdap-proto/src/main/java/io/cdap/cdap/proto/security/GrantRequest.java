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
import io.cdap.cdap.proto.id.EntityId;

import java.util.Set;

/**
 * Request for granting a principal permission to perform certain actions on an entity.
 */
@Beta
public class GrantRequest extends AuthorizationRequest {

  public GrantRequest(Authorizable authorizable, Principal principal, Set<? extends Permission> permissions) {
    super(authorizable, principal, permissions);
    if (principal == null) {
      throw new IllegalArgumentException("principal is required");
    }
    if (permissions == null) {
      throw new IllegalArgumentException("permissions is required");
    }
  }

  public GrantRequest(EntityId entityId, Principal principal, Set<? extends Permission> permissions) {
    this(Authorizable.fromEntityId(entityId), principal, permissions);
  }
}
