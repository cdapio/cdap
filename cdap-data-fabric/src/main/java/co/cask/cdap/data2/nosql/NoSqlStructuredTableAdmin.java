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

import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * The Nosql admin will use the existing dataset framework to create and drop tables.
 */
public final class NoSqlStructuredTableAdmin implements StructuredTableAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(NoSqlStructuredTableAdmin.class);
  private static final DatasetContext SYSTEM_CONTEXT = DatasetContext.from(NamespaceId.SYSTEM.getNamespace());

  static final String ENTITY_TABLE_NAME = "entity.store";

  private final DatasetDefinition tableDefinition;
  private final DatasetSpecification entityTableSpec;
  private final StructuredTableRegistry registry;

  @Inject
  public NoSqlStructuredTableAdmin(@Named(Constants.Dataset.TABLE_TYPE) DatasetDefinition tableDefinition,
                                   StructuredTableRegistry registry) {
    this.tableDefinition = tableDefinition;
    entityTableSpec = tableDefinition.configure(ENTITY_TABLE_NAME, DatasetProperties.EMPTY);
    this.registry = registry;
  }

  @Override
  public void create(StructuredTableSpecification spec) throws IOException, TableAlreadyExistsException {
    LOG.info("Creating table {} in namespace {}", spec.getTableId().getName(), NamespaceId.SYSTEM);
    DatasetAdmin admin = tableDefinition.getAdmin(SYSTEM_CONTEXT, entityTableSpec, null);
    if (!admin.exists()) {
      LOG.info("Creating dataset table {} in namespace {}", entityTableSpec.getName(), NamespaceId.SYSTEM);
      admin.create();
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
      DatasetAdmin admin = tableDefinition.getAdmin(SYSTEM_CONTEXT, entityTableSpec, null);
      LOG.info("Dropping dataset table {} in namespace {}", entityTableSpec.getName(), NamespaceId.SYSTEM);
      admin.drop();
    }
  }

  <T> T getEntityTable(String name) throws IOException {
    if (NoSqlStructuredTableAdmin.ENTITY_TABLE_NAME.equals(name)) {
      //noinspection unchecked
      return (T) tableDefinition.getDataset(SYSTEM_CONTEXT, entityTableSpec, Collections.emptyMap(), null);
    }
    throw new DatasetInstantiationException("Trying to access dataset other than entity table: " + name);
  }
}
