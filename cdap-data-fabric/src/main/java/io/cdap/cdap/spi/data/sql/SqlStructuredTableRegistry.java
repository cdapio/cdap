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

package io.cdap.cdap.spi.data.sql;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.sql.DataSource;

/**
 * SQL implementation of StructuredTableRegistry.
 */
public class SqlStructuredTableRegistry implements StructuredTableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(SqlStructuredTableRegistry.class);
  private static final Gson GSON = new Gson();

  private static final String TABLE_NAME_FIELD = "table_name";
  private static final String TABLE_SPEC_FIELD = "table_specification";
  private static final StructuredTableId REGISTRY = new StructuredTableId("entity_registry");
  private static final StructuredTableSpecification SPEC =
    new StructuredTableSpecification.Builder()
      .withId(REGISTRY)
      .withFields(Fields.stringType(TABLE_NAME_FIELD),
                  Fields.stringType(TABLE_SPEC_FIELD))
      .withPrimaryKeys(TABLE_NAME_FIELD)
      .build();

  private final DataSource dataSource;
  private final TransactionRunner transactionRunner;

  @Inject
  public SqlStructuredTableRegistry(DataSource dataSource) {
    this.dataSource = dataSource;
    this.transactionRunner = createTransactionRunner();
  }

  @Override
  public void initialize() throws IOException {
    createRegistryTable();
  }

  public void registerSpecification(StructuredTableSpecification specification) throws TableAlreadyExistsException {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable registry = context.getTable(REGISTRY);
      StructuredTableId tableId = specification.getTableId();
      Optional<StructuredRow> optional =
        registry.read(Collections.singleton(Fields.stringField(TABLE_NAME_FIELD, tableId.getName())));
      if (optional.isPresent()) {
        throw new TableAlreadyExistsException(tableId);
      }
      LOG.debug("Registering table specification {}", specification);
      registry.upsert(
        Arrays.asList(Fields.stringField(TABLE_NAME_FIELD, tableId.getName()),
                      Fields.stringField(TABLE_SPEC_FIELD, GSON.toJson(specification)))
      );
    }, TableAlreadyExistsException.class);
  }

  @Nullable
  public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable registry = context.getTable(REGISTRY);
      Optional<StructuredRow> optional =
        registry.read(Collections.singleton(Fields.stringField(TABLE_NAME_FIELD, tableId.getName())));
      if (!optional.isPresent()) {
        return null;
      }
      String specString = optional.get().getString(TABLE_SPEC_FIELD);
      LOG.trace("Got specification {} from registry", specString);
      return GSON.fromJson(specString, StructuredTableSpecification.class);
    });
  }

  public void removeSpecification(StructuredTableId tableId) {
    TransactionRunners.run(transactionRunner, context -> {
      StructuredTable registry = context.getTable(REGISTRY);
      registry.delete(Collections.singleton(Fields.stringField(TABLE_NAME_FIELD, tableId.getName())));
    });
  }

  public boolean isEmpty() {
    return TransactionRunners.run(transactionRunner, context -> {
      StructuredTable registry = context.getTable(REGISTRY);
      try (CloseableIterator<StructuredRow> it = registry.scan(Range.all(), Integer.MAX_VALUE)) {
        return !it.hasNext();
      }
    });
  }

  private void createRegistryTable() throws IOException {
    // Use a no-op registry to create PostgresSqlStructuredTableAdmin.
    // During a table creation, except for registerSpecification and getSpecification, no other methods on the
    // registry will be called by PostgresSqlStructuredTableAdmin. Since we always check existence before we create
    // the registry table, we can safely return null for the getSpecification method.
    StructuredTableRegistry noOpRegistry = new StructuredTableRegistry() {
      UnsupportedOperationException exception =
        new UnsupportedOperationException("Not expected to be called during creation of registry!");

      @Override
      public void initialize() {
        throw exception;
      }

      @Override
      public void registerSpecification(StructuredTableSpecification specification) {
        // Do nothing
      }

      @Nullable
      @Override
      public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
        return null;
      }

      @Override
      public void removeSpecification(StructuredTableId tableId) {
        throw exception;
      }

      @Override
      public boolean isEmpty() {
        throw exception;
      }
    };

    try {
      // Create the table if needed
      PostgresSqlStructuredTableAdmin admin = new PostgresSqlStructuredTableAdmin(noOpRegistry, dataSource);
      if (!admin.tableExists(REGISTRY)) {
        LOG.info("Creating SQL table {}", REGISTRY);
        admin.create(SPEC);
      }
    } catch (TableAlreadyExistsException e) {
      // Looks like the table was created concurrently by some other process
      LOG.debug(String.format("Got exception when trying to create table %s", REGISTRY), e);
    }
  }

  private TransactionRunner createTransactionRunner() {
    UnsupportedOperationException exception =
      new UnsupportedOperationException("Unexpected DDL operation during registry usage!!");
    // Create a spec admin that only returns the spec for the registry while creating SqlTransactionRunner
    StructuredTableAdmin specAdmin =
      new StructuredTableAdmin() {
        @Override
        public void create(StructuredTableSpecification spec) {
          throw exception;
        }

        @Override
        public StructuredTableSpecification getSpecification(StructuredTableId tableId) {
          return SPEC;
        }

        @Override
        public void drop(StructuredTableId tableId) {
          throw exception;
        }
      };
    // The metrics collection service might not get started at this moment,
    // so inject a NoopMetricsCollectionService.
    return new SqlTransactionRunner(specAdmin, dataSource, new NoOpMetricsCollectionService(), false);
  }
}
