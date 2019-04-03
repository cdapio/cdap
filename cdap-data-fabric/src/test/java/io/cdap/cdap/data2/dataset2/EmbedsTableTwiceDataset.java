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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A test dataset that reproduces CDAP-3037: If a dataset embeds Table, and also embeds another dataset
 * from a different module that itself embeds a Table, DatasetDefinitionLoader will try to register the
 * Table's module twice (once directly for the embedded Table, once indirectly when adding the dependencies
 * of the other dataset). The second time it will fail with a conflict because the type is already defined.
 * This happens not only for table, but for any dataset that is embedded both directly and transitively
 * through a different module (note this does not happen for IndexedTable, because it is in the same
 * module as Table).
 */
public class EmbedsTableTwiceDataset extends AbstractDataset implements KeyValueTable {
  private static final byte[] COL = new byte[0];

  private final Table table;
  private final PartitionedFileSet files;

  public EmbedsTableTwiceDataset(DatasetSpecification spec,
                                 @EmbeddedDataset("data") Table table,
                                 @EmbeddedDataset("files") PartitionedFileSet index) {
    super(spec.getName(), table, index);
    this.table = table;
    this.files = index;
  }

  public void put(String key, String value) throws Exception {
    table.put(Bytes.toBytes(key), COL, Bytes.toBytes(value));
    files.getPartition(PartitionKey.builder().addField(key, value).build());
  }

  public String get(String key) throws Exception {
    byte[] value = table.get(Bytes.toBytes(key), COL);
    return value == null ? null : Bytes.toString(value);
  }
}
