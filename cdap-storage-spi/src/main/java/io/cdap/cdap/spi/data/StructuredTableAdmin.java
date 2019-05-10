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

package io.cdap.cdap.spi.data;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Defines admin operations on a {@link StructuredTable}.
 */
@Beta
public interface StructuredTableAdmin {
  /**
   * Create a StructuredTable using the {@link StructuredTableSpecification}.
   *
   * @param spec table specification
   * @throws IOException if there is an error creating the table
   * @throws TableAlreadyExistsException if the table already exists
   */
  void create(StructuredTableSpecification spec) throws IOException, TableAlreadyExistsException;

  /**
   * Get the {@link StructuredTableSpecification} corresponding to the given table id.
   *
   * @param tableId the table id
   * @return the specification for the table
   */
  @Nullable
  StructuredTableSpecification getSpecification(StructuredTableId tableId);

  /**
   * Drop the StructuredTable synchronously. After this method is called, the existing table will get deleted. If the
   * table does not exist, no operation will be done.
   *
   * @param tableId the table id
   * @throws IOException if there is an error dropping the table
   */
  void drop(StructuredTableId tableId) throws IOException;
}
