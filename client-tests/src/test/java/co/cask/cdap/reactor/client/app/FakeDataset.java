/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client.app;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */
public class FakeDataset extends AbstractDataset
  implements BatchReadable<byte[], byte[]>, RecordScannable<KeyValue<byte[], byte[]>> {

  public static final String TYPE_NAME = "fakeType";

  private KeyValueTable table;

  public FakeDataset(String instanceName, KeyValueTable table) {
    super(instanceName, table);
    this.table = table;
  }

  public byte[] get(byte[] key) {
    return table.read(key);
  }

  public void put(byte[] key, byte[] value) {
    table.write(key, value);
  }

  @Override
  public Type getRecordType() {
    return table.getRecordType();
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public RecordScanner<KeyValue<byte[], byte[]>> createSplitRecordScanner(Split split) {
    return table.createSplitRecordScanner(split);
  }

  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    return table.createSplitReader(split);
  }
}
