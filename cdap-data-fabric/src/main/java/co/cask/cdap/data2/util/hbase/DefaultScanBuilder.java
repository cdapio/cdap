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

import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Default implementation for {@link ScanBuilder}. Specific HBase compat module can extends and override methods.
 */
class DefaultScanBuilder implements ScanBuilder {

  protected final Scan scan;

  DefaultScanBuilder() {
    this.scan = new Scan();
  }

  DefaultScanBuilder(Scan other) throws IOException {
    this.scan = new Scan(other);
  }

  @Override
  public ScanBuilder addFamily(byte[] family) {
    scan.addFamily(family);
    return this;
  }

  @Override
  public ScanBuilder addColumn(byte[] family, byte[] qualifier) {
    scan.addColumn(family, qualifier);
    return this;
  }

  @Override
  public ScanBuilder setTimeRange(long minStamp, long maxStamp) throws IOException {
    scan.setTimeRange(minStamp, maxStamp);
    return this;
  }

  @Override
  public ScanBuilder setTimeStamp(long timestamp) throws IOException {
    scan.setTimeStamp(timestamp);
    return this;
  }

  @Override
  public ScanBuilder setStartRow(byte[] startRow) {
    scan.setStartRow(startRow);
    return this;
  }

  @Override
  public ScanBuilder setStopRow(byte[] stopRow) {
    scan.setStopRow(stopRow);
    return this;
  }

  @Override
  public ScanBuilder setMaxVersions() {
    scan.setMaxVersions();
    return this;
  }

  @Override
  public ScanBuilder setMaxVersions(int maxVersions) {
    scan.setMaxVersions(maxVersions);
    return this;
  }

  @Override
  public ScanBuilder setFilter(Filter filter) {
    scan.setFilter(filter);
    return this;
  }

  @Override
  public ScanBuilder setFamilyMap(Map<byte[], NavigableSet<byte[]>> familyMap) {
    scan.setFamilyMap(familyMap);
    return this;
  }

  @Override
  public ScanBuilder setAttribute(String name, byte[] value) {
    scan.setAttribute(name, value);
    return this;
  }

  @Override
  public ScanBuilder setId(String id) {
    scan.setId(id);
    return this;
  }

  @Override
  public ScanBuilder setAuthorizations(Authorizations authorizations) {
    scan.setAuthorizations(authorizations);
    return this;
  }

  @Override
  public ScanBuilder setACL(String user, Permission perms) {
    scan.setACL(user, perms);
    return this;
  }

  @Override
  public ScanBuilder setACL(Map<String, Permission> perms) {
    scan.setACL(perms);
    return this;
  }

  @Override
  public ScanBuilder setBatch(int batch) {
    scan.setBatch(batch);
    return this;
  }

  @Override
  public ScanBuilder setMaxResultsPerColumnFamily(int limit) {
    scan.setMaxResultsPerColumnFamily(limit);
    return this;
  }

  @Override
  public ScanBuilder setRowOffsetPerColumnFamily(int offset) {
    scan.setRowOffsetPerColumnFamily(offset);
    return this;
  }

  @Override
  public ScanBuilder setCaching(int caching) {
    scan.setCaching(caching);
    return this;
  }

  @Override
  public ScanBuilder setMaxResultSize(long maxResultSize) {
    scan.setMaxResultSize(maxResultSize);
    return this;
  }

  @Override
  public ScanBuilder setCacheBlocks(boolean cacheBlocks) {
    scan.setCacheBlocks(cacheBlocks);
    return this;
  }

  @Override
  public ScanBuilder setLoadColumnFamiliesOnDemand(boolean value) {
    scan.setLoadColumnFamiliesOnDemand(value);
    return this;
  }

  @Override
  public ScanBuilder setRaw(boolean raw) {
    scan.setRaw(raw);
    return this;
  }

  @Override
  public ScanBuilder setIsolationLevel(IsolationLevel level) {
    scan.setIsolationLevel(level);
    return this;
  }

  @Override
  public ScanBuilder setSmall(boolean small) {
    scan.setSmall(small);
    return this;
  }

  @Override
  public Scan build() {
    return scan;
  }

  @Override
  public String toString() {
    return scan.toString();
  }
}
