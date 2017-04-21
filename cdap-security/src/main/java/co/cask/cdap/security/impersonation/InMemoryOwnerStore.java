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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * In-memory implementation of {@link OwnerStore} used in test cases.
 */
public class InMemoryOwnerStore extends OwnerStore {

  private final Map<NamespacedEntityId, KerberosPrincipalId> ownerInfo = new HashMap<>();

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
  public synchronized KerberosPrincipalId getOwner(NamespacedEntityId entityId) throws IOException {
    validate(entityId);
    return ownerInfo.get(entityId);
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
