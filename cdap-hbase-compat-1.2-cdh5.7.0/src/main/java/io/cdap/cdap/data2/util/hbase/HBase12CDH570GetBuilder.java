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

import org.apache.hadoop.hbase.client.Get;

/**
 * Implementation of {@link GetBuilder} for CDH 5.7.0 version of HBase 1.2.x
 */
class HBase12CDH570GetBuilder extends DefaultGetBuilder {

  HBase12CDH570GetBuilder(byte[] row) {
    super(row);
  }

  HBase12CDH570GetBuilder(Get get) {
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
