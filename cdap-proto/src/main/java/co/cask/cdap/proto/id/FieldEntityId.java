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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.element.EntityType;

/**
 * An {@link NamespacedEntityId} which refers to a specific field.
 */
public abstract class FieldEntityId extends NamespacedEntityId {

  protected final String field;

  protected FieldEntityId(String namespace, String field) {
    super(namespace, EntityType.FIELD);
    if (field == null) {
      throw new NullPointerException("Field cannot be null.");
    }
    ensureValidId("field", field);
    this.field = field;
  }

  public String getField() {
    return field;
  }
}
