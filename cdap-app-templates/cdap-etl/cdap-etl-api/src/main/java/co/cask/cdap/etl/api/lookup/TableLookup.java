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
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.Lookup;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * {@link Lookup} implementation for {@link Table}.
 */
public class TableLookup implements Lookup<Row> {

  private final Table table;

  public TableLookup(Table table) {
    this.table = table;
  }

  @Override
  public Row lookup(String key) {
    return table.get(Bytes.toBytes(key));
  }

  @Override
  public Map<String, Row> lookup(String... keys) {
    return lookup(ImmutableSet.copyOf(keys));
  }

  @Override
  public Map<String, Row> lookup(Set<String> keys) {
    Map<String, Row> results = new HashMap<>();
    for (String key : keys) {
      results.put(key, lookup(key));
    }
    return results;
  }
}
