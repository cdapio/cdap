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

package io.cdap.cdap.spi.data.nosql;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.dataset.lib.IndexedTableDefinition;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * The Nosql admin will use the existing dataset framework to create and drop tables.
 */
public final class NoSqlStructuredTableAdmin implements StructuredTableAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(NoSqlStructuredTableAdmin.class);
  private static final DatasetContext SYSTEM_CONTEXT = DatasetContext.from(NamespaceId.SYSTEM.getNamespace());

  static final String ENTITY_TABLE_NAME = "entity.store";

  private final NoSqlStructuredTableDatasetDefinition indexTableDefinition;
  private final DatasetSpecification indexTableSpec;
  private final StructuredTableRegistry registry;

  @Inject
  public NoSqlStructuredTableAdmin(
    @Named(Constants.Dataset.TABLE_TYPE) DatasetDefinition tableDefinition,
    StructuredTableRegistry registry) {
    //noinspection unchecked - due to the guice binding we know that the tableDefinition is of the right type
    this.indexTableDefinition =
      new NoSqlStructuredTableDatasetDefinition(new IndexedTableDefinition("indexedTable", tableDefinition));
    this.indexTableSpec =
      indexTableDefinition.configure(ENTITY_TABLE_NAME,
                                     DatasetProperties.builder().add(IndexedTable.DYNAMIC_INDEXING, "true").build());
    this.registry = registry;
  }

  @Override
  public void create(StructuredTableSpecification spec) throws IOException, TableAlreadyExistsException {
    if (registry.getSpecification(spec.getTableId()) != null) {
      throw new TableAlreadyExistsException(spec.getTableId());
    }
    LOG.info("Creating table {} in namespace {}", spec, NamespaceId.SYSTEM);
    DatasetAdmin indexTableAdmin = indexTableDefinition.getAdmin(SYSTEM_CONTEXT, indexTableSpec, null);
    if (!indexTableAdmin.exists()) {
      LOG.info("Creating dataset indexed table {} in namespace {}", indexTableSpec.getName(), NamespaceId.SYSTEM);
      indexTableAdmin.create();
    }
    registry.registerSpecification(spec);
  }

  @Override
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    return registry.getSpecification(tableId);
  }

  @Override
  public void drop(StructuredTableId tableId) throws IOException {
    LOG.info("Dropping table {} in namespace {}", tableId.getName(), NamespaceId.SYSTEM);
    registry.removeSpecification(tableId);
    if (registry.isEmpty()) {
      DatasetAdmin admin = indexTableDefinition.getAdmin(SYSTEM_CONTEXT, indexTableSpec, null);
      LOG.info("Dropping dataset indexed table {} in namespace {}", indexTableSpec.getName(), NamespaceId.SYSTEM);
      admin.drop();
    }
  }

  <T> T getEntityTable(Map<String, String> arguments) throws IOException {
    //noinspection unchecked
    return (T) indexTableDefinition.getDataset(SYSTEM_CONTEXT, indexTableSpec, arguments, null);
  }
}
