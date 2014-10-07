/*
 * *
 *  Copyright © 2014 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 * /
 */

package co.cask.cdap.test.app;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Table;
import com.sun.tools.javac.resources.version;

import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */

public class FakeDataset extends AbstractDataset {

  private KeyValueTable uniqueCountTable;


  public FakeDataset(DatasetSpecification spec,
                          @EmbeddedDataset("unique") KeyValueTable uniqueCountTable) {
    super(spec.getName(), uniqueCountTable);
    this.uniqueCountTable = uniqueCountTable;
  }

  public void put(byte[] key, byte[] value) {
    uniqueCountTable.write(key, value);
  }

  @Override
  public int getVersion() {
    return 1;
  }
}

