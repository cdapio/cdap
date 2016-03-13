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

package co.cask.cdap.security.spi.authorization;

import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;

import java.util.Set;

/**
 * A no-op authorizer to use when authorization is disabled.
 */
public class NoOpAuthorizer implements Authorizer {
  @Override
  public void enforce(EntityId entity, Principal principal, Action action) throws UnauthorizedException {
    // no-op
  }

  @Override
  public void grant(EntityId entity, Principal principal, Set<Action> actions) {
    //no-op
  }

  @Override
  public void revoke(EntityId entity, Principal principal, Set<Action> actions) {
    //no-op
  }

  @Override
  public void revoke(EntityId entity) {
    //no-op
  }
}
