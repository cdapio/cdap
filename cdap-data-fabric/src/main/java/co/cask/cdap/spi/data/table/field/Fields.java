/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.spi.data.table.field;

/**
 * Convenience methods to work on {@link Field} and {@link FieldType}.
 */
public final class Fields {
  /**
   * @return the FieldType of INTEGER with the given name.
   */
  public static FieldType integer(String name) {
    return new FieldType(name, FieldType.Type.INTEGER);
  }

  /**
    * @return true if the type is allowed to be part of a primary key, false otherwise.
   */
  public static boolean isPrimaryKeyType(FieldType.Type type) {
    return FieldType.PRIMARY_KEY_TYPES.contains(type);
  }
}
