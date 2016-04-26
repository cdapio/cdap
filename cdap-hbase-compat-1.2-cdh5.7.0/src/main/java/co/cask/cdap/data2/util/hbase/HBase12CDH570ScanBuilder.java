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

import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of {@link ScanBuilder} for CDH 5.7.0 version of HBase 1.2.x
 */
class HBase12CDH570ScanBuilder extends DefaultScanBuilder {

  HBase12CDH570ScanBuilder() {
    super();
  }

  HBase12CDH570ScanBuilder(Scan other) throws IOException {
    super(other);
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
  public ScanBuilder setCacheBlocks(boolean cacheBlocks) {
    scan.setCacheBlocks(cacheBlocks);
    return this;
  }

  @Override
  public ScanBuilder setCaching(int caching) {
    scan.setCaching(caching);
    return this;
  }

  @Override
  public ScanBuilder setIsolationLevel(IsolationLevel level) {
    scan.setIsolationLevel(level);
    return this;
  }

  @Override
  public ScanBuilder setLoadColumnFamiliesOnDemand(boolean value) {
    scan.setLoadColumnFamiliesOnDemand(value);
    return this;
  }

  @Override
  public ScanBuilder setMaxResultSize(long maxResultSize) {
    scan.setMaxResultSize(maxResultSize);
    return this;
  }

  @Override
  public ScanBuilder setMaxResultsPerColumnFamily(int limit) {
    scan.setMaxResultsPerColumnFamily(limit);
    return this;
  }

  @Override
  public ScanBuilder setRaw(boolean raw) {
    scan.setRaw(raw);
    return this;
  }

  @Override
  public ScanBuilder setRowOffsetPerColumnFamily(int offset) {
    scan.setRowOffsetPerColumnFamily(offset);
    return this;
  }

  @Override
  public ScanBuilder setSmall(boolean small) {
    scan.setSmall(small);
    return this;
  }
}
