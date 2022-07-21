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

import com.google.common.base.Joiner;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.metrics.MetricsCollector;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.StructuredTableInstantiationException;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.common.MetricStructuredTable;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * The nosql context to get the table.
 */
public class NoSqlStructuredTableContext implements StructuredTableContext {
  private final NoSqlStructuredTableAdmin tableAdmin;
  private final DatasetContext datasetContext;
  private final MetricsCollector metricsCollector;
  private final boolean emitTimeMetrics;

  NoSqlStructuredTableContext(NoSqlStructuredTableAdmin tableAdmin, DatasetContext datasetContext,
                              MetricsCollector metricsCollector, boolean emitTimeMetrics) {
    this.tableAdmin = tableAdmin;
    this.datasetContext = datasetContext;
    this.metricsCollector = metricsCollector;
    this.emitTimeMetrics = emitTimeMetrics;
  }

  @Override
  public StructuredTable getTable(StructuredTableId tableId)
    throws StructuredTableInstantiationException, TableNotFoundException {
    try {
      StructuredTableSchema schema = tableAdmin.getSchema(tableId);

      Map<String, String> arguments = new HashMap<>();
      if (schema.getIndexes().isEmpty()) {
        // No indexes on the table
        arguments.put(IndexedTable.INDEX_COLUMNS_CONF_KEY, "");
        arguments.put(IndexedTable.DYNAMIC_INDEXING_PREFIX, "");
      } else {
        arguments.put(IndexedTable.INDEX_COLUMNS_CONF_KEY, Joiner.on(",").join(schema.getIndexes()));
        arguments.put(IndexedTable.DYNAMIC_INDEXING_PREFIX, tableId.getName());
      }
      StructuredTable table =
        new NoSqlStructuredTable(datasetContext.getDataset(NoSqlStructuredTableAdmin.ENTITY_TABLE_NAME, arguments),
                                 schema);
      return new MetricStructuredTable(tableId, table, metricsCollector, emitTimeMetrics);
    } catch (DatasetInstantiationException e) {
      throw new StructuredTableInstantiationException(
        tableId, String.format("Error instantiating table %s", tableId), e);
    }
  }
}
