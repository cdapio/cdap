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

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.data2.dataset2.lib.table.MetricsTable;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.data.common.StructuredTableRegistry;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * NoSQL implementation of StructuredTableRegistry.
 */
public class NoSqlStructuredTableRegistry implements StructuredTableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(NoSqlStructuredTableAdmin.class);
  private static final DatasetContext SYSTEM_CONTEXT = DatasetContext.from(NamespaceId.SYSTEM.getNamespace());

  private static final String ENTITY_REGISTRY = "entity.registry";
  private static final byte[] TABLE_ROWKEY_PREFIX = {'t'};
  private static final byte[] SCHEMA_COL_BYTES = Bytes.toBytes("schema");
  private static final byte[][] SCHEMA_COL_BYTES_ARRAY = new byte[][] {SCHEMA_COL_BYTES};
  private static final Gson GSON = new Gson();

  private final NoSqlStructuredTableDatasetDefinition tableDefinition;
  private final DatasetSpecification entityRegistrySpec;

  private volatile boolean initialized;

  @VisibleForTesting
  public NoSqlStructuredTableRegistry(DatasetDefinition tableDefinition) {
    this.tableDefinition = new NoSqlStructuredTableDatasetDefinition(tableDefinition);
    this.entityRegistrySpec = tableDefinition.configure(ENTITY_REGISTRY, DatasetProperties.EMPTY);
  }

  private void initIfNeeded() throws IOException {
    if (initialized) {
      return;
    }
    synchronized (this) {
      if (initialized) {
        return;
      }

      DatasetAdmin admin = tableDefinition.getAdmin(SYSTEM_CONTEXT, entityRegistrySpec, null);
      if (!admin.exists()) {
        LOG.info("Creating dataset table {} in namespace {}", entityRegistrySpec.getName(), NamespaceId.SYSTEM);
        admin.create();
      }
      initialized = true;
    }
  }

  @Override
  public void registerSpecification(StructuredTableSpecification specification) {
    StructuredTableId tableId = specification.getTableId();
    MetricsTable table = getRegistryTable();
    try {
      byte[] rowKeyBytes = getRowKeyBytes(tableId);
      byte[] oldValue = table.get(rowKeyBytes, SCHEMA_COL_BYTES);
      if (oldValue != null) {
        LOG.debug("Updating table specification {}", specification);
      } else {
        LOG.debug("Registering table specification {}", specification);
      }
      byte[] serialized = Bytes.toBytes(GSON.toJson(specification));
      table.swap(rowKeyBytes, SCHEMA_COL_BYTES, oldValue, serialized);
    } finally {
      closeRegistryTable(table);
    }
  }

  @Nullable
  @Override
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    MetricsTable table = getRegistryTable();
    try {
      byte[] serialized = table.get(getRowKeyBytes(tableId), SCHEMA_COL_BYTES);
      return serialized == null ? null : GSON.fromJson(Bytes.toString(serialized), StructuredTableSpecification.class);
    } finally {
      closeRegistryTable(table);
    }
  }

  @Override
  public void removeSpecification(StructuredTableId tableId) {
    LOG.debug("Removing table specification for table {}", tableId);
    MetricsTable table = getRegistryTable();
    try {
      table.delete(getRowKeyBytes(tableId), SCHEMA_COL_BYTES_ARRAY);
    } finally {
      closeRegistryTable(table);
    }
  }

  @Override
  public boolean isEmpty() {
    MetricsTable table = getRegistryTable();
    try {
      try (Scanner scanner = table.scan(TABLE_ROWKEY_PREFIX, Bytes.stopKeyForPrefix(TABLE_ROWKEY_PREFIX), null)) {
        return scanner.next() == null;
      }
    } finally {
      closeRegistryTable(table);
    }
  }

  private static byte[] getRowKeyBytes(StructuredTableId tableId) {
   return Bytes.concat(TABLE_ROWKEY_PREFIX, Bytes.toBytes(tableId.getName()));
  }

  private <T extends Dataset> T getRegistryTable() {
    try {
      initIfNeeded();
      //noinspection unchecked
      return (T) tableDefinition.getDataset(SYSTEM_CONTEXT, entityRegistrySpec, Collections.emptyMap(), null);
    } catch (IOException e) {
      throw new DatasetInstantiationException(
        String.format("Cannot instantiate entity registry table %s", entityRegistrySpec.getName()), e);
    }
  }

  private void closeRegistryTable(MetricsTable table) {
    try {
      table.close();
    } catch (IOException e) {
      LOG.debug("Got exception while closing table {}", ENTITY_REGISTRY, e);
    }
  }
}
