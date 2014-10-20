/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.inmemory;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Result;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * An in-memory implememtation of a scanner.
 */
public class InMemoryScanner implements Scanner {

  private final Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows;
  private final Set<byte[]> columnsToInclude;
  private final FuzzyRowFilter filter;

  public InMemoryScanner(Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows) {
    this(rows, null, null);
  }

  public InMemoryScanner(Iterator<Map.Entry<byte[], NavigableMap<byte[], byte[]>>> rows, FuzzyRowFilter filter,
                         byte[][] columnsToInclude) {
    this.rows = rows;
    this.filter = filter;
    if (columnsToInclude != null) {
      this.columnsToInclude = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
      Collections.addAll(this.columnsToInclude, columnsToInclude);
    } else {
      this.columnsToInclude = null;
    }
  }

  @Override
  public Row next() {
    Map<byte[], byte[]> columns = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    Map.Entry<byte[], NavigableMap<byte[], byte[]>> rowEntry = null;

    while (columns.isEmpty() && this.rows.hasNext()) {
      rowEntry = this.rows.next();
      if (filter != null) {
        FuzzyRowFilter.ReturnCode code = filter.filterRow(rowEntry.getKey());
        if (FuzzyRowFilter.ReturnCode.DONE.equals(code)) {
          break; // no more rows can match
        } else if (!FuzzyRowFilter.ReturnCode.INCLUDE.equals(code)) {
          continue; // this row does not match filter, move to next row
        }
      }
      //Try to read all columns for this row
      for (Map.Entry<byte[], byte[]> colEntry : rowEntry.getValue().entrySet()) {
        if (columnsToInclude == null || columnsToInclude.contains(colEntry.getKey())) {
          columns.put(colEntry.getKey(), colEntry.getValue());
        }
      }
    }
    if (columns.size() > 0) {
      assert rowEntry != null;
      return new Result(rowEntry.getKey(), columns);
    } else {
      return null;
    }
  }

  @Override
  public void close() { /* no op */ }

}
