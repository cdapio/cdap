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

package co.cask.cdap.api.dataset.table;

import co.cask.cdap.api.common.Bytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for defining a row with set of columns.
 * @param <T> implementor type - to support handy builder-like methods.
 */
public abstract class RowColumns<T> {
  private final byte[] row;

  private final List<byte[]> columns;

  public byte[] getRow() {
    return row;
  }

  public List<byte[]> getColumns() {
    return columns;
  }

  // key & column as byte[]

  public RowColumns(byte[] row) {
    this.row = row;
    this.columns = new ArrayList<>();
  }

  public RowColumns(byte[] row, byte[]... columns) {
    this(row);
    add(columns);
  }

  @SuppressWarnings("unchecked")
  public T add(byte[]... columns) {
    Collections.addAll(this.columns, columns);
    return (T) this;
  }

  // key and column as String

  public RowColumns(String row) {
    this(Bytes.toBytes(row));
  }

  public RowColumns(String row, String... columns) {
    this(row);
    add(columns);
  }

  @SuppressWarnings("unchecked")
  public T add(String... columns) {
    for (String col : columns) {
      this.columns.add(Bytes.toBytes(col));
    }
    return (T) this;
  }
}
