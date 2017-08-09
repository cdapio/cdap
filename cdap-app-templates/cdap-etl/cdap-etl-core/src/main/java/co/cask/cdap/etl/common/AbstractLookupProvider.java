/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.lookup.KeyValueTableLookup;
import co.cask.cdap.etl.api.lookup.TableLookup;
import co.cask.cdap.etl.api.lookup.TableRecordLookup;
import co.cask.cdap.format.RowRecordTransformer;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * {@link Lookup} that provides common functionality.
 */
public abstract class AbstractLookupProvider implements LookupProvider {

  @SuppressWarnings("unchecked")
  protected <T> Lookup<T> getLookup(String table, @Nullable Dataset dataset, Admin admin) {
    if (dataset == null) {
      throw new RuntimeException(String.format("Dataset %s does not exist", table));
    }

    DatasetProperties props = getDatasetProperties(admin, table);
    Schema schema = getSchema(props);
//    new ReflectionRowRecordReader(schema, TableProperties.getRowFieldName(props));
    if (dataset instanceof KeyValueTable) {
      return (Lookup<T>) new KeyValueTableLookup((KeyValueTable) dataset);
    } else if (dataset instanceof Table) {
//      return (Lookup<T>) new TableLookup((Table) dataset, schema);
      final RowRecordTransformer rowRecordTransformer =
        new RowRecordTransformer(schema, TableProperties.getRowFieldName(props));
      return (Lookup<T>) new TableRecordLookup((Table) dataset, schema,
                                   new Function<Row, StructuredRecord>() {
                                     @Nullable
                                     @Override
                                     public StructuredRecord apply(Row input) {
                                       return rowRecordTransformer.toRecord(input);
                                     }
                                   });
    } else {
      throw new RuntimeException(String.format("Dataset %s does not support lookup", table));
    }
  }

  private DatasetProperties getDatasetProperties(Admin admin, String table) {
    try {
      return admin.getDatasetProperties(table);
    } catch (DatasetManagementException e) {
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  private Schema getSchema(DatasetProperties dsProps) {
    try {
      String schemaString = dsProps.getProperties().get(DatasetProperties.SCHEMA);
      if (schemaString == null) {
        return null;
      }
      return Schema.parseJson(schemaString);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
