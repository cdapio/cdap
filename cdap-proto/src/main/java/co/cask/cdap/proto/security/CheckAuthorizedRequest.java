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

package co.cask.cdap.proto.security;

import co.cask.cdap.proto.id.EntityId;
import com.google.common.base.Preconditions;

import java.util.Set;

/**
 * Request for checking if a user can perform certain actions on an entity.
 */
public class CheckAuthorizedRequest extends AuthorizationRequest {

  public CheckAuthorizedRequest(EntityId entity, String user, Set<Action> actions) {
    super(entity, user, actions);
    Preconditions.checkArgument(user != null, "user is required");
    Preconditions.checkArgument(actions != null, "actions is required");
  }

  public CheckAuthorizedRequest(CheckAuthorizedRequest other) {
    this(other.getEntity(), other.getUser(), other.getActions());
  }
}
