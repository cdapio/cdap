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

package co.cask.cdap.data2.spi;

import javax.annotation.Nullable;

/**
 * Represents a row of data from a {@link StructuredTable}.
 */
public interface Row {
  /**
   * @return the value of the fieldName as integer.
   * @throws InvalidFieldException if the fieldName is not part of the table schema, or the type does not match.
   */
  @Nullable
  Integer getInteger(String fieldName) throws InvalidFieldException;

  /**
   * @return the value of the fieldName as long.
   * @throws InvalidFieldException if the fieldName is not part of the table schema, or the type does not match.
   */
  @Nullable
  Long getLong(String fieldName) throws InvalidFieldException;

  /**
   * @return the value of the fieldName as string.
   * @throws InvalidFieldException if the fieldName is not part of the table schema, or the type does not match.
   */
  @Nullable
  String getString(String fieldName) throws InvalidFieldException;

  /**
   * @return the value of the fieldName as float.
   * @throws InvalidFieldException if the fieldName is not part of the table schema, or the type does not match.
   */
  @Nullable
  Float getFloat(String fieldName) throws InvalidFieldException;

  /**
   * @return the value of the fieldName as double.
   * @throws InvalidFieldException if the fieldName is not part of the table schema, or the type does not match.
   */
  @Nullable
  Double getDouble(String fieldName) throws InvalidFieldException;
}
