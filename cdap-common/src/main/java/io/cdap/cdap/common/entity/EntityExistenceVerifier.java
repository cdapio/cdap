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

package co.cask.cdap.common.entity;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.id.EntityId;

/**
 * Interface to check if an {@link EntityId} exists in CDAP.
 *
 * @param <ENTITY> the entity type for which to ensure existence.
 */
public interface EntityExistenceVerifier<ENTITY extends EntityId> {
  /**
   * Ensures that the specified {@link EntityId} exists.
   *
   * @param entityId the {@link EntityId} whose existence is to be ensured
   * @throws NotFoundException if the specified {@link EntityId} does not exist
   */
  void ensureExists(ENTITY entityId) throws NotFoundException;
}
