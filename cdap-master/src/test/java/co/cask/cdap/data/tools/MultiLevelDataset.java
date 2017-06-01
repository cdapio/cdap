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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.data2.dataset2.KeyValueTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A dataset that contains complex embedded datasets.
 */
class MultiLevelDataset extends AbstractDataset implements KeyValueTable {
  private static final byte[] COL = new byte[0];

  private final IndexedTable table;
  private final PartitionedFileSet files;

  public MultiLevelDataset(DatasetSpecification spec,
                           @EmbeddedDataset("index-table") IndexedTable table,
                           @EmbeddedDataset("files") PartitionedFileSet files) {
    super(spec.getName(), table, files);
    this.table = table;
    this.files = files;
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
