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

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.EntityId;

/**
 * Thrown when an element already exists.
 */
public class AlreadyExistsException extends ConflictException {

  private final Object objectId;

  public AlreadyExistsException(Id id) {
    super(String.format("'%s' already exists", id));
    this.objectId = id;
  }

  public AlreadyExistsException(EntityId entityId) {
    super(String.format("'%s' already exists", entityId));
    this.objectId = entityId;
  }

  public AlreadyExistsException(Id id, String message) {
    super(message);
    this.objectId = id;
  }

  public Object getObjectId() {
    return objectId;
  }
}
