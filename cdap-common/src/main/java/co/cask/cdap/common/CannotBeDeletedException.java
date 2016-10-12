/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common;

import co.cask.cdap.proto.id.EntityId;

/**
 * Thrown when an element cannot be deleted.
 */
public class CannotBeDeletedException extends ConflictException {

  private final EntityId entityId;
  private final String reason;

  public CannotBeDeletedException(EntityId id) {
    super(String.format("'%s' could not be deleted", id));
    this.entityId = id;
    this.reason = null;
  }

  public CannotBeDeletedException(EntityId id, String reason) {
    super(String.format("'%s' could not be deleted. Reason: %s", id, reason));
    this.entityId = id;
    this.reason = reason;
  }

  public CannotBeDeletedException(EntityId id, Throwable cause) {
    super(String.format("'%s' could not be deleted. Reason: %s", id, cause.getMessage()), cause);
    this.entityId = id;
    this.reason = cause.getMessage();
  }

  public EntityId getEntityId() {
    return entityId;
  }

  public String getReason() {
    return reason;
  }
}
