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

package io.cdap.cdap.spi.data.sql;

import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.StructuredTableInstantiationException;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.common.MetricStructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;

import java.io.IOException;
import java.sql.Connection;

/**
 * The Sql context to get the table.
 */
public class SqlStructuredTableContext implements StructuredTableContext {
  private final StructuredTableAdmin admin;
  private final Connection connection;
  private final MetricsCollector metricsCollector;
  private final boolean emitTimeMetrics;

  public SqlStructuredTableContext(StructuredTableAdmin structuredTableAdmin, Connection connection,
                                   MetricsCollector metricsCollector, boolean emitTimeMetrics) {
    this.admin = structuredTableAdmin;
    this.connection = connection;
    this.metricsCollector = metricsCollector;
    this.emitTimeMetrics = emitTimeMetrics;
  }

  @Override
  public StructuredTable getTable(
    StructuredTableId tableId) throws StructuredTableInstantiationException, TableNotFoundException {
    StructuredTableSpecification specification = admin.getSpecification(tableId);
    if (specification == null) {
      throw new TableNotFoundException(tableId);
    }
    return new MetricStructuredTable(
      tableId, new PostgresSqlStructuredTable(connection, new StructuredTableSchema(specification)),
      metricsCollector, emitTimeMetrics);
  }

  @Override
  public StructuredTable getOrCreateTable(StructuredTableId tableId, StructuredTableSpecification tableSpec)
    throws StructuredTableInstantiationException, IOException, TableNotFoundException {
    StructuredTableSpecification specification = admin.getSpecification(tableId);
    if (specification == null) {
      try {
        admin.create(tableSpec);
      } catch (TableAlreadyExistsException e) {
        // Possibly due to a race condition if this call isn't running in a transaction and someone else just created
        // the table, in such case just proceed.
      }
    }
    // Note that getTable call may fail with TableNotFoundException for the same reason:
    // this call isn't running in a transaction and getTable call races with someone else's table deletion operation.
    return getTable(tableId);
  }
}
