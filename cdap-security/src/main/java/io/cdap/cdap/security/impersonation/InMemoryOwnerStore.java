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

package io.cdap.cdap.security.impersonation;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespacedEntityId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * In-memory implementation of {@link OwnerStore} used in test cases.
 */
public class InMemoryOwnerStore extends OwnerStore {

  private final Map<NamespacedEntityId, KerberosPrincipalId> ownerInfo = new HashMap<>();

  @VisibleForTesting
  synchronized void clear() {
    ownerInfo.clear();
  }

  @Override
  public synchronized void add(NamespacedEntityId entityId,
                               KerberosPrincipalId kerberosPrincipalId) throws AlreadyExistsException {
    validate(entityId, kerberosPrincipalId);
    if (ownerInfo.containsKey(entityId)) {
      throw new AlreadyExistsException(entityId,
                                       String.format("Owner information already exists for entity '%s'.", entityId));
    }
    ownerInfo.put(entityId, kerberosPrincipalId);
  }

  @Nullable
  @Override
  public synchronized KerberosPrincipalId getOwner(NamespacedEntityId entityId) {
    validate(entityId);
    return ownerInfo.get(entityId);
  }

  @Override
  public synchronized <T extends NamespacedEntityId> Map<T, KerberosPrincipalId> getOwners(Set<T> ids) {
    ids.forEach(this::validate);
    Map<T, KerberosPrincipalId> result = new HashMap<>();
    for (T id : ids) {
      KerberosPrincipalId principalId = ownerInfo.get(id);
      if (principalId != null) {
        result.put(id, principalId);
      }
    }
    return result;
  }

  @Override
  public synchronized boolean exists(NamespacedEntityId entityId) throws IOException {
    validate(entityId);
    return ownerInfo.containsKey(entityId);
  }

  @Override
  public synchronized void delete(NamespacedEntityId entityId) throws IOException {
    validate(entityId);
    ownerInfo.remove(entityId);
  }
}
