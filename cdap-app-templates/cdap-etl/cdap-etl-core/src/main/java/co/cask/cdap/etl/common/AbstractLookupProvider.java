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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.lookup.KeyValueTableLookup;
import co.cask.cdap.etl.api.lookup.TableLookup;
import com.google.common.base.Throwables;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * {@link Lookup} that provides common functionality.
 */
public abstract class AbstractLookupProvider implements LookupProvider {

  @SuppressWarnings("unchecked")
  protected <T, R> Lookup<T, R> getLookup(String table, @Nullable Dataset dataset, Admin admin) {
    if (dataset == null) {
      throw new RuntimeException(String.format("Dataset %s does not exist", table));
    }

    Schema schema = getSchema(admin, table);
    if (dataset instanceof KeyValueTable) {
      return (Lookup<T, R>) new KeyValueTableLookup((KeyValueTable) dataset, schema);
    } else if (dataset instanceof Table) {
      return (Lookup<T, R>) new TableLookup((Table) dataset, schema);
    } else {
      throw new RuntimeException(String.format("Dataset %s does not support lookup", table));
    }
  }

  @Nullable
  private Schema getSchema(Admin admin, String table) {
    try {
      DatasetProperties dsProps = admin.getDatasetProperties(table);
      String schemaString = dsProps.getProperties().get(DatasetProperties.SCHEMA);
      if (schemaString == null) {
        return null;
      }
      return Schema.parseJson(schemaString);
    } catch (IOException | DatasetManagementException e) {
      throw Throwables.propagate(e);
    }
  }
}
