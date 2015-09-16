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

import org.apache.hadoop.hbase.client.Get;

/**
 * HBase 1.1 specific implementation for {@link GetBuilder}.
 */
class HBase11GetBuilder extends DefaultGetBuilder {

  HBase11GetBuilder(byte[] row) {
    super(row);
  }

  HBase11GetBuilder(Get get) {
    super(get);
  }

  @Override
  public GetBuilder setCheckExistenceOnly(boolean checkExistenceOnly) {
    get.setCheckExistenceOnly(checkExistenceOnly);
    return this;
  }

  @Override
  public GetBuilder setClosestRowBefore(boolean closestRowBefore) {
    get.setClosestRowBefore(closestRowBefore);
    return this;
  }

  @Override
  public GetBuilder setAttribute(String name, byte[] value) {
    get.setAttribute(name, value);
    return this;
  }

  @Override
  public GetBuilder setCacheBlocks(boolean cacheBlocks) {
    get.setCacheBlocks(cacheBlocks);
    return this;
  }

  @Override
  public GetBuilder setId(String id) {
    get.setId(id);
    return this;
  }
}
