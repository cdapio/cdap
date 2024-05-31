/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.util.hbase;

import org.apache.hadoop.hbase.client.Delete;

/**
 * HBase 1.0 specific implementation for {@link DeleteBuilder}.
 */
class HBase10CDH550DeleteBuilder extends DefaultDeleteBuilder {

  HBase10CDH550DeleteBuilder(byte[] row) {
    super(row);
  }

  HBase10CDH550DeleteBuilder(Delete delete) {
    super(delete);
  }

  @Override
  public DeleteBuilder setAttribute(String name, byte[] value) {
    delete.setAttribute(name, value);
    return this;
  }

  @Override
  public DeleteBuilder setId(String id) {
    delete.setId(id);
    return this;
  }

  @Override
  public DeleteBuilder setTimestamp(long timestamp) {
    delete.setTimestamp(timestamp);
    return this;
  }
}
