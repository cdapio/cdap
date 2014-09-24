/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.webanalytics;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

/**
 * A {@link Dataset} that stores IP to total visit counts. It uses a {@link KeyValueTable}
 * underneath to hold the data. It also implements the {@link RecordScannable} interface so
 * that it can be queried using ad-hoc SQL.
 */
public class UniqueVisitCount extends AbstractDataset implements RecordScannable<KeyValue<String, Long>> {

  private final KeyValueTable keyValueTable;

  /**
   * Constructor for the Dataset. Request to use a {@link KeyValueTable}.
   *
   * @param spec The specification of the Dataset instance.
   * @param keyValueTable The underlying table
   */
  public UniqueVisitCount(DatasetSpecification spec, @EmbeddedDataset("kv") KeyValueTable keyValueTable) {
    super(spec.getName(), keyValueTable);
    this.keyValueTable = keyValueTable;
  }

  /**
   * Performs increments of visit count of the given IP.
   *
   * @param ip The IP to increment
   * @param amount The amount to increment
   */
  public void increment(String ip, long amount) {
    // Delegates to the system KeyValueTable for actual storage operation
    keyValueTable.increment(Bytes.toBytes(ip), amount);
  }

  /**
   * Reads the visit count for a given IP.
   *
   * @param ip The IP to lookup
   * @return the number of visits
   */
  public long getCount(String ip) {
    byte[] value = keyValueTable.read(Bytes.toBytes(ip));
    return (value == null) ? 0L : Bytes.toLong(value);
  }

  @Override
  public Type getRecordType() {
    return new TypeToken<KeyValue<String, Long>>() { }.getType();
  }

  @Override
  public List<Split> getSplits() {
    return keyValueTable.getSplits();
  }

  @Override
  public RecordScanner<KeyValue<String, Long>> createSplitRecordScanner(Split split) {
    // When scanning records, simply convert the type from the underlying KeyValueTable into <String, Long> pair.
    final RecordScanner<KeyValue<byte[], byte[]>> scanner = keyValueTable.createSplitRecordScanner(split);
    return new RecordScanner<KeyValue<String, Long>>() {

      @Override
      public void initialize(Split split) throws InterruptedException {
        scanner.initialize(split);
      }

      @Override
      public boolean nextRecord() throws InterruptedException {
        return scanner.nextRecord();
      }

      @Override
      public KeyValue<String, Long> getCurrentRecord() throws InterruptedException {
        KeyValue<byte[], byte[]> record = scanner.getCurrentRecord();
        return new KeyValue<String, Long>(Bytes.toString(record.getKey()), Bytes.toLong(record.getValue()));
      }

      @Override
      public void close() {
        scanner.close();
      }
    };
  }
}
