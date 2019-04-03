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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.DatasetStatePersistor;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * An implementation of {@link DatasetStatePersistor} that uses a row of a {@link KeyValueTable} for persistence.
 */
public class KVTableStatePersistor implements DatasetStatePersistor {

  private final String kvTableName;
  private final String rowKey;

  public KVTableStatePersistor(String kvTableName, String rowKey) {
    this.kvTableName = kvTableName;
    this.rowKey = rowKey;
  }

  @Override
  public byte[] readState(DatasetContext datasetContext) {
    return getKVTable(datasetContext).read(rowKey);
  }

  @Override
  public void persistState(DatasetContext datasetContext, byte[] state) {
    getKVTable(datasetContext).write(rowKey, state);
  }

  private KeyValueTable getKVTable(DatasetContext datasetContext) {
    return datasetContext.getDataset(kvTableName);
  }
}
