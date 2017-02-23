/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.hbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.io.IOException;
import java.util.List;

/**
 * {@link Filter} that encapsulates another {@link Filter}. It remembers the last {@link KeyValue}
 * for which the underlying filter returned the {@link ReturnCode#NEXT_COL} or {@link ReturnCode#INCLUDE_AND_NEXT_COL},
 * so that when {@link #filterKeyValue} is called again for the same {@link KeyValue} with different
 * version, it returns {@link ReturnCode#NEXT_COL} directly without consulting the underlying {@link Filter}.
 * Please see TEPHRA-169 for more details.
 */
public class CellSkipFilter extends FilterBase {
  private final Filter filter;
  // remember the previous keyvalue processed by filter when the return code was NEXT_COL or INCLUDE_AND_NEXT_COL
  private KeyValue skipColumn = null;

  public CellSkipFilter(Filter filter) {
    this.filter = filter;
  }

  /**
   * Determines whether the current cell should be skipped. The cell will be skipped
   * if the previous keyvalue had the same key as the current cell. This means filter already responded
   * for the previous keyvalue with ReturnCode.NEXT_COL or ReturnCode.INCLUDE_AND_NEXT_COL.
   * @param cell the {@link Cell} to be tested for skipping
   * @return true is current cell should be skipped, false otherwise
   */
  private boolean skipCellVersion(Cell cell) {
    return skipColumn != null
      && CellUtil.matchingRow(cell, skipColumn.getRowArray(), skipColumn.getRowOffset(),
                              skipColumn.getRowLength())
      && CellUtil.matchingFamily(cell, skipColumn.getFamilyArray(), skipColumn.getFamilyOffset(),
                                 skipColumn.getFamilyLength())
      && CellUtil.matchingQualifier(cell, skipColumn.getQualifierArray(), skipColumn.getQualifierOffset(),
                                    skipColumn.getQualifierLength());
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    if (skipCellVersion(cell)) {
      return ReturnCode.NEXT_COL;
    }

    ReturnCode code = filter.filterKeyValue(cell);
    if (code == ReturnCode.NEXT_COL || code == ReturnCode.INCLUDE_AND_NEXT_COL) {
      // only store the reference to the keyvalue if we are returning NEXT_COL or INCLUDE_AND_NEXT_COL
      skipColumn = KeyValueUtil.createFirstOnRow(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
                                                   cell.getFamilyArray(), cell.getFamilyOffset(),
                                                   cell.getFamilyLength(), cell.getQualifierArray(),
                                                   cell.getQualifierOffset(), cell.getQualifierLength());
    } else {
      skipColumn = null;
    }
    return code;
  }

  @Override
  public boolean filterRow() throws IOException {
    return filter.filterRow();
  }

  @Override
  public Cell transformCell(Cell cell) throws IOException {
    return filter.transformCell(cell);
  }

  @Override
  public void reset() throws IOException {
    filter.reset();
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
    return filter.filterRowKey(buffer, offset, length);
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    return filter.filterAllRemaining();
  }

  @Override
  public void filterRowCells(List<Cell> kvs) throws IOException {
    filter.filterRowCells(kvs);
  }

  @Override
  public boolean hasFilterRow() {
    return filter.hasFilterRow();
  }

  @SuppressWarnings("deprecation")
  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) throws IOException {
    return filter.getNextKeyHint(currentKV);
  }

  @Override
  public Cell getNextCellHint(Cell currentKV) throws IOException {
    return filter.getNextCellHint(currentKV);
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    return filter.isFamilyEssential(name);
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return filter.toByteArray();
  }
}
