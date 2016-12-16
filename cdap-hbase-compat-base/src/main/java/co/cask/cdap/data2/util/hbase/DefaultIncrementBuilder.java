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

package co.cask.cdap.data2.util.hbase;

import co.cask.cdap.api.common.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Increment;

import java.io.IOException;

/**
 * Default implementation of {@link IncrementBuilder}. Specific HBase compat module can extends and override methods.
 */
class DefaultIncrementBuilder implements IncrementBuilder {

  protected final Increment increment;

  DefaultIncrementBuilder(byte[] row) {
    this.increment = new Increment(row);
  }

  @Override
  public IncrementBuilder add(byte[] family, byte[] qualifier, long value) {
    increment.addColumn(family, qualifier, value);
    return this;
  }

  @Override
  public IncrementBuilder add(byte[] family, byte[] qualifier, long ts, long value) throws IOException {
    increment.add(new KeyValue(increment.getRow(), family, qualifier, ts, Bytes.toBytes(value)));
    return this;
  }

  @Override
  public IncrementBuilder setAttribute(String name, byte[] value) {
    increment.setAttribute(name, value);
    return this;
  }

  @Override
  public boolean isEmpty() {
    return increment.isEmpty();
  }

  @Override
  public Increment build() {
    return increment;
  }

  @Override
  public String toString() {
    return increment.toString();
  }
}
