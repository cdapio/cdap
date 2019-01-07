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

package co.cask.cdap.data2.nosql;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.StructuredTable;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.StructuredTableContext;
import co.cask.cdap.spi.data.TableInstantiationException;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.TableSchema;

/**
 *
 */
public class NoSqlStructuredTableContext implements StructuredTableContext {
  private final NamespaceId namespaceId;
  private final DatasetContext datasetContext;
  private final StructuredTableAdmin tableAdmin;

  public NoSqlStructuredTableContext(NamespaceId namespaceId, DatasetContext datasetContext,
                                     StructuredTableAdmin tableAdmin) {
    this.namespaceId = namespaceId;
    this.datasetContext = datasetContext;
    this.tableAdmin = tableAdmin;
  }

  @Override
  public StructuredTable getTable(StructuredTableId tableId) throws TableInstantiationException {
    try {
      return new NoSqlStructuredTable(datasetContext.getDataset(namespaceId.getNamespace(), tableId.getName()),
                                      new TableSchema(tableAdmin.getSpecification(tableId)));
    } catch (DatasetInstantiationException e) {
      throw new TableInstantiationException(String.format("Error instantiating table %s", tableId), e);
    }
  }
}
