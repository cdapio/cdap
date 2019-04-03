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

package co.cask.cdap.data2.dataset2.cache;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.tephra.Transaction;

import java.util.Map;

public class TestDataset extends AbstractDataset implements Comparable<TestDataset> {

  private final KeyValueTable kvTable;
  private final Map<String, String> arguments;
  private final String key;
  private final String value;

  private Transaction currentTx = null;
  private boolean isClosed = false;

  public TestDataset(DatasetSpecification spec, KeyValueTable kv, Map<String, String> args) {
    super(spec.getName(), kv);
    this.kvTable = kv;
    this.arguments = ImmutableSortedMap.copyOf(args);
    this.key = arguments.get("key") == null ? "key" : arguments.get("key");
    this.value = arguments.get("value") == null ? "value" : arguments.get("value");
  }

  public void write() throws Exception {
    kvTable.write(key, value);
  }

  public String read() throws Exception {
    return Bytes.toString(kvTable.read(key));
  }

  public KeyValueTable getKeyValueTable() {
    return kvTable;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  @Override
  public void startTx(Transaction tx) {
    currentTx = tx;
    super.startTx(tx);
  }

  @Override
  public boolean commitTx() throws Exception {
    currentTx = null;
    return super.commitTx();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    currentTx = null;
    return super.rollbackTx();
  }

  public Map<String, String> getArguments() {
    return arguments;
  }

  @Override
  public String getName() {
    return super.getName();
  }

  @Override
  public int compareTo(TestDataset other) {
    int result = getName().compareTo(other.getName());
    if (result != 0) {
      return result;
    }
    result = getArguments().toString().compareTo(other.getArguments().toString());
    if (result != 0) {
      return result;
    }
    return Integer.compare(hashCode(), other.hashCode());
  }

  @Override
  public void close() {
    isClosed = true;
  }

  public Transaction getCurrentTransaction() {
    return currentTx;
  }

  public boolean isClosed() {
    return isClosed;
  }
}
