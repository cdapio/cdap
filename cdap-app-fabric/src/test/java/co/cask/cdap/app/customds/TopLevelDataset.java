/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.customds;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;

/**
 * A top level dataset class.
 */
public class TopLevelDataset extends AbstractDataset {

  private final Table table;
  private final KeyValueTable kvTable;

  public TopLevelDataset(DatasetSpecification spec, @EmbeddedDataset("table") Table table,
                         @EmbeddedDataset("kv")KeyValueTable kvTable) {
    super(spec.getName(), table);
    this.table = table;
    this.kvTable = kvTable;
  }

  public void put(String row, String column, String value) {
    table.put(new Put(row, column, value));
    kvTable.write(row, value);
  }

  public String get(String row, String column) {
    return table.get(new Get(row, column)).getString(column) + " " + Bytes.toString(kvTable.read(row));
  }
}
