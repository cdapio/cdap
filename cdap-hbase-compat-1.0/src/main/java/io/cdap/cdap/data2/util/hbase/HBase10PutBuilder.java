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

package co.cask.cdap.data2.util.hbase;

import org.apache.hadoop.hbase.client.Put;

/**
 * HBase 1.0 specific implementation for {@link PutBuilder}.
 */
class HBase10PutBuilder extends DefaultPutBuilder {

  HBase10PutBuilder(Put put) {
    super(put);
  }

  HBase10PutBuilder(byte[] row) {
    super(row);
  }

  @Override
  public PutBuilder setAttribute(String name, byte[] value) {
    put.setAttribute(name, value);
    return this;
  }

  @Override
  public PutBuilder setId(String id) {
    put.setId(id);
    return this;
  }
}
