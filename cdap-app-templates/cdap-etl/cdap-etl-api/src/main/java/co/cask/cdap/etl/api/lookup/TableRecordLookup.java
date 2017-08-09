/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api.lookup;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.Lookup;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * {@link Lookup} implementation for {@link Table}.
 */
public class TableRecordLookup implements Lookup<StructuredRecord> {

  private final Table table;
  private final Schema schema;
  private final Function<Row, StructuredRecord> rowRecordTransformer;

  public TableRecordLookup(Table table, Schema schema, Function<Row, StructuredRecord> rowRecordTransformer) {
    this.table = table;
    this.schema = schema;
    this.rowRecordTransformer = rowRecordTransformer;
  }

  /**
   * @return Schema of the dataset, or null if no schema is stored in the dataset's properties
   */
  @Nullable
  public Schema getSchema() {
    return schema;
  }

  @Override
  public StructuredRecord lookup(byte[] key) {
    return toRecord(table.get(key));
  }

  @Override
  public Map<byte[], StructuredRecord> lookup(byte[]... keys) {
    List<Get> gets = new ArrayList<>();
    for (byte[] key : keys) {
      gets.add(new Get(key));
    }
    List<Row> rows = table.get(gets);

    Map<byte[], StructuredRecord> result = new HashMap<>(keys.length);
    for (int i = 0; i < keys.length; i++) {
      result.put(keys[i], toRecord(rows.get(i)));
    }
    return result;
  }

  private StructuredRecord toRecord(Row row) {
    return rowRecordTransformer.apply(row);
  }

  @Override
  public StructuredRecord lookup(String key) {
    return lookup(Bytes.toBytes(key));
  }

  @Override
  public Map<String, StructuredRecord> lookup(String... keys) {
    return lookup(ImmutableSet.copyOf(keys));
  }

  @Override
  public Map<String, StructuredRecord> lookup(Set<String> keys) {
    Map<String, StructuredRecord> results = new HashMap<>();
    for (String key : keys) {
      results.put(key, lookup(key));
    }
    return results;
  }
}
