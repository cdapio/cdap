/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Builder for creating {@link Scan}. This builder should be used for cross HBase versions compatibility.
 * All methods on this class are just delegating to calls to {@link Scan} object.
 */
public interface ScanBuilder {

  ScanBuilder addFamily(byte[] family);

  ScanBuilder addColumn(byte[] family, byte[] qualifier);

  ScanBuilder setTimeRange(long minStamp, long maxStamp) throws IOException;

  ScanBuilder setTimeStamp(long timestamp) throws IOException;

  ScanBuilder setStartRow(byte[] startRow);

  ScanBuilder setStopRow(byte[] stopRow);

  ScanBuilder setMaxVersions();

  ScanBuilder setMaxVersions(int maxVersions);

  ScanBuilder setFilter(Filter filter);

  ScanBuilder setFamilyMap(Map<byte[], NavigableSet<byte[]>> familyMap);

  ScanBuilder setAttribute(String name, byte[] value);

  ScanBuilder setId(String id);

  ScanBuilder setAuthorizations(Authorizations authorizations);

  ScanBuilder setACL(String user, Permission perms);

  ScanBuilder setACL(Map<String, Permission> perms);

  ScanBuilder setBatch(int batch);

  ScanBuilder setMaxResultsPerColumnFamily(int limit);

  ScanBuilder setRowOffsetPerColumnFamily(int offset);

  ScanBuilder setCaching(int caching);

  ScanBuilder setMaxResultSize(long maxResultSize);

  ScanBuilder setCacheBlocks(boolean cacheBlocks);

  ScanBuilder setLoadColumnFamiliesOnDemand(boolean value);

  ScanBuilder setRaw(boolean raw);

  ScanBuilder setIsolationLevel(IsolationLevel level);

  ScanBuilder setSmall(boolean small);

  Scan build();
}
