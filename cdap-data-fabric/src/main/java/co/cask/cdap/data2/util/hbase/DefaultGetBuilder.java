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
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * Default implementation of {@link GetBuilder}. Specific HBase compat module can extends and override methods.
 */
class DefaultGetBuilder implements GetBuilder {

  protected final Get get;

  DefaultGetBuilder(byte[] row) {
    this.get = new Get(row);
  }

  DefaultGetBuilder(Get get) {
    this.get = new Get(get);
  }

  @Override
  public GetBuilder addFamily(byte[] family) {
    get.addFamily(family);
    return this;
  }

  @Override
  public GetBuilder addColumn(byte[] family, byte[] qualifier) {
    get.addColumn(family, qualifier);
    return this;
  }

  @Override
  public GetBuilder setTimeRange(long minStamp, long maxStamp) throws IOException {
    get.setTimeRange(minStamp, maxStamp);
    return this;
  }

  @Override
  public GetBuilder setTimeStamp(long timestamp) throws IOException {
    get.setTimeStamp(timestamp);
    return this;
  }

  @Override
  public GetBuilder setMaxVersions() {
    get.setMaxVersions();
    return this;
  }

  @Override
  public GetBuilder setMaxVersions(int maxVersions) throws IOException {
    get.setMaxVersions(maxVersions);
    return this;
  }

  @Override
  public GetBuilder setMaxResultsPerColumnFamily(int limit) {
    get.setMaxResultsPerColumnFamily(limit);
    return this;
  }

  @Override
  public GetBuilder setRowOffsetPerColumnFamily(int offset) {
    get.setRowOffsetPerColumnFamily(offset);
    return this;
  }

  @Override
  public GetBuilder setFilter(Filter filter) {
    get.setFilter(filter);
    return this;
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
  public GetBuilder setId(String id) {
    get.setId(id);
    return this;
  }

  @Override
  public GetBuilder setCacheBlocks(boolean cacheBlocks) {
    get.setCacheBlocks(cacheBlocks);
    return this;
  }

  @Override
  public Get build() {
    return get;
  }

  @Override
  public String toString() {
    return get.toString();
  }
}
