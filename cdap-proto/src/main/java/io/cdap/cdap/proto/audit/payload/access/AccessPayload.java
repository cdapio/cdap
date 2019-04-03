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

package co.cask.cdap.proto.audit.payload.access;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.id.EntityId;

import java.util.Objects;

/**
 * Represents the access that happened on a data entity.
 */
@Beta
public class AccessPayload extends AuditPayload {
  private final AccessType accessType;
  private final EntityId accessor;

  public AccessPayload(AccessType accessType, EntityId accessor) {
    this.accessType = accessType;
    this.accessor = accessor;
  }

  public AccessType getAccessType() {
    return accessType;
  }

  public EntityId getAccessor() {
    return accessor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AccessPayload)) {
      return false;
    }
    AccessPayload that = (AccessPayload) o;
    return Objects.equals(accessType, that.accessType) &&
      Objects.equals(accessor, that.accessor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessType, accessor);
  }

  @Override
  public String toString() {
    return "AccessPayload{" +
      "accessType=" + accessType +
      ", accessor=" + accessor +
      "} " + super.toString();
  }
}
