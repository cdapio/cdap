/*
 * Copyright © 2019 Cask Data, Inc.
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

/**
 * This interface provides methods that instantiate a {@link StructuredTable} during the runtime.
 * of a program.
 */
@Beta
public interface StructuredTableContext {

  /**
   * Get an instance of the specified Dataset.
   *
   * @param tableId the table id
   * @return An instance of the specified table, never null
   * @throws StructuredTableInstantiationException if the table cannot be instantiated
   * @throws TableNotFoundException if the table is not found
   */
  StructuredTable getTable(StructuredTableId tableId)
    throws StructuredTableInstantiationException, TableNotFoundException;

  /**
   * Get an instance of the specified Dataset. If none exist, create one based on the given
   * {@link StructuredTableSpecification} first and then returns it.
   *
   * @param tableId the table id
   * @return An instance of the specified table, never null
   * @throws StructuredTableInstantiationException if the table cannot be instantiated
   * @throws TableNotFoundException if table was deleted after existence check, therefore leading to table not found
   * @throws IOException if table creation failed
   */
  StructuredTable getOrCreateTable(StructuredTableId tableId, StructuredTableSpecification tableSpec)
    throws StructuredTableInstantiationException, IOException, TableNotFoundException;
}

