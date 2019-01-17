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

package co.cask.cdap.spi.data;

import co.cask.cdap.spi.data.table.field.Field;

import java.util.Collection;
import javax.annotation.Nullable;

/**
 * Represents a row of data from a {@link StructuredTable}.
 */
public interface StructuredRow {
  /**
   * @return the value of the field named fieldName as integer, or null if the field value is not defined
   * @throws InvalidFieldException if the fieldName is not part of the table schema, or is of incompatible type
   */
  @Nullable
  Integer getInteger(String fieldName) throws InvalidFieldException;

  /**
   * @return the value of the field named fieldName as long, or null if the field value is not defined
   * @throws InvalidFieldException if the fieldName is not part of the table schema,
   * or is of incompatible type. Integer field will be automatically widened to Long.
   */
  @Nullable
  Long getLong(String fieldName) throws InvalidFieldException;

  /**
   * @return the value of the field named fieldName as string, or null if the field value is not defined
   * @throws InvalidFieldException if the fieldName is not part of the table schema,
   * or is of incompatible type. Numeric to string conversion will not be done.
   */
  @Nullable
  String getString(String fieldName) throws InvalidFieldException;

  /**
   * @return the value of the field named fieldName as float, or null if the field value is not defined
   * @throws InvalidFieldException if the fieldName is not part of the table schema, or is of incompatible type
   */
  @Nullable
  Float getFloat(String fieldName) throws InvalidFieldException;

  /**
   * @return the value of the field named fieldName as double, or null if the field value is not defined
   * @throws InvalidFieldException if the fieldName is not part of the table schema,
   * or is of incompatible type. Float field will be automatically widened to Double.
   */
  @Nullable
  Double getDouble(String fieldName) throws InvalidFieldException;

  /**
   * @return the primary keys of this row
   */
  Collection<Field<?>> getPrimaryKeys();
}
