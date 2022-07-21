/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.common.entity;

import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.util.Map;

/**
 * Default implementation of {@link EntityExistenceVerifier}.
 */
public class DefaultEntityExistenceVerifier implements EntityExistenceVerifier<EntityId> {
  private final Map<Class<? extends EntityId>, EntityExistenceVerifier<? extends EntityId>> existenceVerifiers;

  @Inject
  DefaultEntityExistenceVerifier(Map<Class<? extends EntityId>,
                                     EntityExistenceVerifier<? extends EntityId>> existenceVerifiers) {
    this.existenceVerifiers = existenceVerifiers;
  }

  @Override
  public void ensureExists(EntityId entityId) throws NotFoundException, UnauthorizedException {
    getVerifier(entityId).ensureExists(entityId);
  }

  @SuppressWarnings("unchecked")
  private <T extends EntityId> EntityExistenceVerifier<T> getVerifier(T entitiyId) {
    return (EntityExistenceVerifier<T>) existenceVerifiers.get(entitiyId.getClass());
  }
}

