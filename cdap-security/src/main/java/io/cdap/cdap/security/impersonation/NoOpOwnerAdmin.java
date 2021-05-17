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

import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespacedEntityId;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * No-op implementation of {@link OwnerAdmin}. This is just a dummy OwnerAdmin which can be used in unit tests.
 * Although, this binding should not be used if the the unit test needs app fabric, stream admin or
 * dataset instance service as they need a functional OwnerAdmin.
 */
public class NoOpOwnerAdmin implements OwnerAdmin {
  @Override
  public void add(NamespacedEntityId entityId,
                  KerberosPrincipalId kerberosPrincipalId) throws AlreadyExistsException {
    // no-op
  }

  @Nullable
  @Override
  public KerberosPrincipalId getOwner(NamespacedEntityId entityId) {
    return null;
  }

  @Nullable
  @Override
  public String getOwnerPrincipal(NamespacedEntityId entityId) {
    return null;
  }

  @Override
  public <T extends NamespacedEntityId> Map<T, String> getOwnerPrincipals(Set<T> ids) {
    return Collections.emptyMap();
  }

  @Nullable
  @Override
  public ImpersonationInfo getImpersonationInfo(NamespacedEntityId entityId) {
    return null;
  }

  @Nullable
  @Override
  public String getImpersonationPrincipal(NamespacedEntityId entityId) {
    return null;
  }

  @Override
  public boolean exists(NamespacedEntityId entityId) {
    return false;
  }

  @Override
  public void delete(NamespacedEntityId entityId) {
    // no-op
  }
}
