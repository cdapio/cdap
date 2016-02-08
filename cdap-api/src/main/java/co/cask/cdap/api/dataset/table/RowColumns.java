/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Base class for defining a row with set of columns.
 * @param <T> implementor type - to support handy builder-like methods.
 */
public abstract class RowColumns<T> {
  private final byte[] row;

  private List<byte[]> columns;

  // key & column as byte[]

  /**
   * Defines a row with all columns included. If any of the {@code add} method is called, only columns specified through
   * those {@code add} method calls are included.
   * @param row the row to define.
   */
  public RowColumns(byte[] row) {
    this.row = row;
  }

  /**
   * Defines a row, with the specified set of columns.
   * @param row the row to define
   * @param columns the set of columns of the row to included
   */
  public RowColumns(byte[] row, byte[]... columns) {
    this.row = row;
    this.columns = Arrays.asList(columns);
  }

  // key and column as String

  /**
   * Defines a row with all columns included. If any of the {@code add} method is called, only columns specified through
   * those {@code add} method calls are included.
   * @param row the row to define. Note: It will be converted to {@code byte[]} with UTF-8 encoding.
   */
  public RowColumns(String row) {
    this(Bytes.toBytes(row));
  }

  /**
   * Defines a row, including all columns, unless additional columns are specified.
   * Note that the provided strings will be converted to {@code byte[]} with UTF-8 encoding.
   *
   * @param row the row to define
   * @param columns the set of columns of the row to included
   */
  public RowColumns(String row, String... columns) {
    this(row);
    add(columns);
  }

  /**
   * Specifies column(s) to be included.
   * @param firstColumn the first column being included
   * @param moreColumns a collection of additional columns being included
   * @return the {@link RowColumns} object being defined
   */
  @SuppressWarnings("unchecked")
  public T add(byte[] firstColumn, byte[]... moreColumns) {
    initArray();
    this.columns.add(firstColumn);
    Collections.addAll(this.columns, moreColumns);
    return (T) this;
  }

  /**
   * Specifies column(s) to be included.
   * @param columns a collection of columns being included
   * @return the {@link RowColumns} object being defined
   */
  @SuppressWarnings("unchecked")
  public T add(byte[][] columns) {
    initArray();
    Collections.addAll(this.columns, columns);
    return (T) this;
  }

  /**
   * Specifies column(s) to be included.
   * Note that the provided strings will be converted to {@code byte[]} with UTF-8 encoding.
   * @param firstColumn the first column being included
   * @param moreColumns a collection of additional columns being included
   * @return the {@link RowColumns} object being defined
   */
  @SuppressWarnings("unchecked")
  public T add(String firstColumn, String... moreColumns) {
    initArray();
    this.columns.add(Bytes.toBytes(firstColumn));
    for (String col : moreColumns) {
      this.columns.add(Bytes.toBytes(col));
    }
    return (T) this;
  }

  /**
   * Specifies column(s) to be included.
   * @param columns a collection of columns being included
   * @return the {@link RowColumns} object being defined
   */
  @SuppressWarnings("unchecked")
  public T add(String[] columns) {
    initArray();
    for (String col : columns) {
      this.columns.add(Bytes.toBytes(col));
    }
    return (T) this;
  }

  /**
   * @deprecated As of CDAP 3.3.1, no parameter {@code #add} method is deprecated. Use either {@link #add(String[])} or
   * {@link #add(byte[][])}.
   *
   * @return the {@link RowColumns} object being defined
   */
  @SuppressWarnings("unchecked")
  public T add() {
    // here only for backwards compatibility in 3.3.1.
    return (T) this;
  }

  /**
   * @return the row that was included
   */
  public byte[] getRow() {
    return row;
  }

  /**
   * @return a list of columns of this object. null indicates ALL columns.
   */
  @Nullable
  public List<byte[]> getColumns() {
    return columns;
  }

  private void initArray() {
    if (this.columns == null) {
      this.columns = new ArrayList<>();
    }
  }
}
