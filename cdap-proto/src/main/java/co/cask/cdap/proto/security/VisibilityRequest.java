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

package co.cask.cdap.proto.security;

import co.cask.cdap.proto.id.EntityId;

import java.util.Collections;
import java.util.Set;

/**
 * Request for the visibility check
 */
public class VisibilityRequest {

  private final Principal principal;
  private final Set<EntityId> entityIds;

  public VisibilityRequest(Principal principal, Set<? extends EntityId> entityIds) {
    if (principal == null) {
      throw new IllegalArgumentException("principal is required");
    }
    this.principal = principal;
    this.entityIds = Collections.unmodifiableSet(entityIds);
  }

  public Principal getPrincipal() {
    return principal;
  }

  public Set<EntityId> getEntityIds() {
    return entityIds;
  }
}
