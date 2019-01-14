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

package co.cask.cdap.data2.nosql;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import co.cask.cdap.spi.data.table.StructuredTableSpecificationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The Nosql admin will use the existing dataset framework to create and drop tables.
 */
public final class NoSqlStructuredTableAdmin implements StructuredTableAdmin {
  static final String ENTITY_TABLE_NAME = "entity.store";
  private static final Logger LOG = LoggerFactory.getLogger(NoSqlStructuredTableAdmin.class);

  private final DatasetFramework datasetFramework;

  public NoSqlStructuredTableAdmin(DatasetFramework datasetFramework) {
    this.datasetFramework = datasetFramework;
  }

  @Override
  public void create(StructuredTableSpecification spec) throws IOException {
    LOG.info("Creating table {} in namespace {}", spec, NamespaceId.SYSTEM);
    try {
      DatasetId datasetInstanceId = NamespaceId.SYSTEM.dataset(ENTITY_TABLE_NAME);
      if (!datasetFramework.hasInstance(datasetInstanceId)) {
        datasetFramework.addInstance(Table.class.getName(), datasetInstanceId, DatasetProperties.EMPTY);
      }
      DatasetAdmin admin = datasetFramework.getAdmin(datasetInstanceId, null);
      if (admin == null) {
        throw new IOException(String.format("Error creating table %s. Cannot get DatasetAdmin", spec.getTableId()));
      }
      if (!admin.exists()) {
        admin.create();
      }
    } catch (DatasetManagementException e) {
      throw new IOException(String.format("Error creating table %s", spec.getTableId()), e);
    }
    StructuredTableSpecificationRegistry.registerSpecification(spec);
  }

  @Override
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    return StructuredTableSpecificationRegistry.getSpecification(tableId);
  }

  @Override
  public void drop(StructuredTableId tableId) throws IOException {
    LOG.info("Dropping table {} in namespace {}", tableId, NamespaceId.SYSTEM);
    try {
      DatasetId datasetInstanceId = NamespaceId.SYSTEM.dataset(ENTITY_TABLE_NAME);
      DatasetAdmin admin = datasetFramework.getAdmin(datasetInstanceId, null);
      if (admin == null) {
        throw new IOException(String.format("Error dropping table %s. Cannot get DatasetAdmin", tableId));
      }
      admin.drop();
      datasetFramework.deleteInstance(datasetInstanceId);
      StructuredTableSpecificationRegistry.removeSpecification(tableId);
    } catch (DatasetManagementException e) {
      throw new IOException(String.format("Error dropping table %s", tableId), e);
    }
  }
}
