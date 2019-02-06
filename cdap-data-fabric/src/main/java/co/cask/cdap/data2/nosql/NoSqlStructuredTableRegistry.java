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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.data.TableAlreadyExistsException;
import co.cask.cdap.spi.data.table.StructuredTableId;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.spi.data.table.StructuredTableSpecification;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
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
  private static final DatasetId REGISTRY_DATASET_ID = NamespaceId.SYSTEM.dataset(ENTITY_REGISTRY);
  private static final byte[] TABLE_ROWKEY_PREFIX = {'t'};
  private static final byte[] SCHEMA_COL_BYTES = Bytes.toBytes("schema");
  private static final Gson GSON = new Gson();

  private final DatasetDefinition tableDefinition;
  private final DatasetSpecification entityRegistrySpec;
  private final Transactional transactional;

  @Inject
  public NoSqlStructuredTableRegistry(
    @Named(Constants.Dataset.TABLE_TYPE) DatasetDefinition tableDefinition, TransactionSystemClient txClient) {
    this.tableDefinition = tableDefinition;
    this.entityRegistrySpec = tableDefinition.configure(ENTITY_REGISTRY, DatasetProperties.EMPTY);
    this.transactional = Transactions.createTransactionalWithRetry(
      NoSqlTransactionRunner.createTransactional(txClient, this::getRegistryTable),
      RetryStrategies.retryOnConflict(20, 100));
  }

  @Override
  public void initialize() throws IOException {
    DatasetAdmin admin = tableDefinition.getAdmin(SYSTEM_CONTEXT, entityRegistrySpec, null);
    if (!admin.exists()) {
      LOG.info("Creating dataset table {} in namespace {}", entityRegistrySpec.getName(), NamespaceId.SYSTEM);
      admin.create();
    }
  }

  @Override
  public void registerSpecification(StructuredTableSpecification specification) throws TableAlreadyExistsException {
    LOG.debug("Registering table specification {}", specification);
    StructuredTableId tableId = specification.getTableId();
    Transactionals.execute(
      transactional,
      context -> {
        Table table = context.getDataset(REGISTRY_DATASET_ID.getDataset());
        byte[] serialized = table.get(getRowKeyBytes(tableId), SCHEMA_COL_BYTES);
        if (serialized != null) {
          throw new TableAlreadyExistsException(tableId);
        }
        serialized = Bytes.toBytes(GSON.toJson(specification));
        table.put(getRowKeyBytes(tableId), SCHEMA_COL_BYTES, serialized);
      }, TableAlreadyExistsException.class);
  }

  @Nullable
  @Override
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    return Transactionals.execute(
      transactional,
      context -> {
        Table table = context.getDataset(REGISTRY_DATASET_ID.getDataset());
        byte[] serialized = table.get(getRowKeyBytes(tableId), SCHEMA_COL_BYTES);
        if (serialized == null) {
          return null;
        }
        return GSON.fromJson(Bytes.toString(serialized), StructuredTableSpecification.class);
      }
    );
  }

  @Override
  public void removeSpecification(StructuredTableId tableId) {
    LOG.debug("Removing table specification for table {}", tableId);
    Transactionals.execute(
      transactional,
      context -> {
        Table table = context.getDataset(REGISTRY_DATASET_ID.getDataset());
        table.delete(getRowKeyBytes(tableId));
      });
  }

  @Override
  public boolean isEmpty() {
    return Transactionals.execute(
      transactional,
      context -> {
        Table table = context.getDataset(REGISTRY_DATASET_ID.getDataset());
        try (Scanner scanner = table.scan(TABLE_ROWKEY_PREFIX, Bytes.stopKeyForPrefix(TABLE_ROWKEY_PREFIX))) {
          return scanner.next() == null;
        }
      }
    );
  }

  private static byte[] getRowKeyBytes(StructuredTableId tableId) {
   return Bytes.concat(TABLE_ROWKEY_PREFIX, Bytes.toBytes(tableId.getName()));
  }

  private <T extends Dataset> T getRegistryTable(String name) throws IOException {
    if (ENTITY_REGISTRY.equals(name)) {
      //noinspection unchecked
      return (T) tableDefinition.getDataset(SYSTEM_CONTEXT, entityRegistrySpec, Collections.emptyMap(), null);
    }
    throw new DatasetInstantiationException("Trying to access dataset other than entity registry table: " + name);
  }
}
