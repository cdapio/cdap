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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.entity.EntityExistenceVerifier;
import co.cask.cdap.proto.id.EntityId;

import java.util.Set;

/**
 * An in-memory {@link EntityExistenceVerifier} for use in tests.
 */
public class InMemoryEntityExistenceVerifier implements EntityExistenceVerifier {
  private final Set<EntityId> entities;

  public InMemoryEntityExistenceVerifier(Set<EntityId> entities) {
    this.entities = entities;
  }

  @Override
  public void ensureExists(EntityId entityId) throws NotFoundException {
    if (!entities.contains(entityId)) {
      throw new NotFoundException(entityId.toId());
    }
  }
}
