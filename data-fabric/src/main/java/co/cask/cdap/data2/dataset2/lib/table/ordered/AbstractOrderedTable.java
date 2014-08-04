/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.ordered;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.OrderedTable;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.NavigableMap;

/**
 * Implements some of the methods in a generic way (not necessarily in most efficient way).
 */
public abstract class AbstractOrderedTable implements OrderedTable {
  // empty immutable row's column->value map constant
  // Using ImmutableSortedMap instead of Maps.unmodifiableNavigableMap to avoid conflicts with
  // Hadoop, which uses an older version of guava without that method.
  protected static final NavigableMap<byte[], byte[]> EMPTY_ROW_MAP =
    ImmutableSortedMap.<byte[], byte[]>orderedBy(Bytes.BYTES_COMPARATOR).build();

  @Override
  public byte[] get(byte[] row, byte[] column) throws Exception {
    Map<byte[], byte[]> result = get(row, new byte[][]{column});
    return result.isEmpty() ? null : result.get(column);
  }

  @Override
  public void put(byte [] row, byte [] column, byte[] value) throws Exception {
    put(row, new byte[][] {column}, new byte[][] {value});
  }

  @Override
  public long increment(byte[] row, byte[] column, long amount) throws Exception {
    return increment(row, new byte[][] {column}, new long[] {amount}).get(column);
  }

  @Override
  public void incrementWrite(byte[] row, byte[] column, long amount) throws Exception {
    incrementWrite(row, new byte[][] {column}, new long[] {amount});
  }

  @Override
  public void delete(byte[] row, byte[] column) throws Exception {
    delete(row, new byte[][] {column});
  }
}
