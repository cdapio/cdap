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

package co.cask.cdap.internal.io;

import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;

/**
*
*/
final class FieldEntry {
  private final TypeToken<?> type;
  private final String fieldName;

  FieldEntry(TypeToken<?> type, String fieldName) {
    this.type = type;
    this.fieldName = fieldName;
  }

  public TypeToken<?> getType() {
    return type;
  }

  public String getFieldName() {
    return fieldName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FieldEntry other = (FieldEntry) o;
    return type.equals(other.type) && fieldName.equals(other.fieldName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, fieldName);
  }
}
