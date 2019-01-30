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

package co.cask.cdap.data2.sql;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.StructuredTableInstantiationException;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableSchema;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.store.StoreDefinition;

import java.io.IOException;
import java.sql.Connection;

/**
 * The Sql context to get the table.
 */
public class SqlStructuredTableContext implements StructuredTableContext {
  private final StructuredTableAdmin admin;
  private final Connection connection;

  public SqlStructuredTableContext(StructuredTableAdmin structuredTableAdmin, Connection connection) {
    this.admin = structuredTableAdmin;
    this.connection = connection;
  }

  @Override
  public StructuredTable getTable(
    StructuredTableId tableId) throws StructuredTableInstantiationException, NotFoundException {
    StructuredTableSpecification specification = admin.getSpecification(tableId);
    if (specification == null) {
        // CDAP-14832 Temporarily auto-create the tables
        specification = StoreDefinition.TABLE_REGISTRY.get(tableId);
        if (specification == null) {
          throw new NotFoundException(tableId);
        }
        try {
          admin.create(specification);
        } catch (IOException e) {
          throw new StructuredTableInstantiationException(tableId,
                                                          String.format("Error instantiating table %s", tableId), e);
        } catch (AlreadyExistsException e) {
          // Ignore AlreadyExistsException
        }
    }
    return new PostgresSqlStructuredTable(connection, new StructuredTableSchema(specification));
  }
}
