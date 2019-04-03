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

import org.apache.hadoop.hbase.client.Delete;

/**
 * Implementation of {@link DeleteBuilder} for CDH 5.7.0 of HBase 1.2.x
 */
class HBase12CDH570DeleteBuilder extends DefaultDeleteBuilder {

  HBase12CDH570DeleteBuilder(byte[] row) {
    super(row);
  }

  HBase12CDH570DeleteBuilder(Delete delete) {
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
