/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.dataset.lib.table;

import com.continuuity.api.common.Bytes;
import com.continuuity.tephra.Transaction;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.NavigableMap;

/**
 *
 */
public abstract class BackedByVersionedStoreOcTableClient extends BufferingOcTableClient {
  protected BackedByVersionedStoreOcTableClient(String name, ConflictDetection level) {
    super(name, level);
  }

  public BackedByVersionedStoreOcTableClient(String name) {
    super(name);
  }

  protected static NavigableMap<byte[], byte[]> getLatestNotExcluded(
    NavigableMap<byte[], NavigableMap<Long, byte[]>> rowMap, Transaction tx) {

    // todo: for some subclasses it is ok to do changes in place...
    NavigableMap<byte[], byte[]> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : rowMap.entrySet()) {
      // NOTE: versions map already sorted, first comes latest version
      // todo: not cool to rely on external implementation specifics
      for (Map.Entry<Long, byte[]> versionAndValue : column.getValue().entrySet()) {
        // NOTE: we know that excluded versions are ordered
        if (tx == null || tx.isVisible(versionAndValue.getKey())) {
          result.put(column.getKey(), versionAndValue.getValue());
          break;
        }
      }
    }

    return result;
  }

  protected static NavigableMap<byte[], NavigableMap<byte[], byte[]>> getLatestNotExcludedRows(
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rows, Transaction tx) {
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> result = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

    for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMap : rows.entrySet()) {
      NavigableMap<byte[], byte[]> visibleRowMap = getLatestNotExcluded(rowMap.getValue(), tx);
      if (visibleRowMap.size() > 0) {
        result.put(rowMap.getKey(), visibleRowMap);
      }
    }

    return result;
  }
}
