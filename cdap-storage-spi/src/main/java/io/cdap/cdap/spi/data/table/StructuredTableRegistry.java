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

package co.cask.cdap.spi.data.table;

import co.cask.cdap.spi.data.TableAlreadyExistsException;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Registry of structured table specification.
 */
public interface StructuredTableRegistry {
  /**
   * Initializes the underlying storage for the registry.
   *
   * @throws IOException if not able to write to the underlying storage
   */
  void initialize() throws IOException;

  /**
   * Register a table specification.
   *
   * @param specification table specification to register
   * @throws IOException if not able to write to the underlying storage
   * @throws TableAlreadyExistsException if the table already exists
   */
  void registerSpecification(StructuredTableSpecification specification)
    throws IOException, TableAlreadyExistsException;

  /**
   * Get the specification of a table if it exists in the registry.
   *
   * @param tableId structured table id
   * @return table specification if it exists, null if the table does not exist
   */
  @Nullable
  StructuredTableSpecification getSpecification(StructuredTableId tableId);

  /**
   * Remove the specification of a table from the registry.
   *
   * @param tableId structured table id
   */
  void removeSpecification(StructuredTableId tableId);

  /**
   *
   * @return true if registry does not have any tables registered, false otherwise
   */
  boolean isEmpty();
}
