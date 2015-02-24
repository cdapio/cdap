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
package co.cask.cdap.cli.util.table;

import co.cask.cdap.cli.util.RowMaker;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Represents a table with a header.
 */
public class Table {

  private final List<String> header;
  private final List<String[]> rows;

  /**
   * @param header strings representing the header of the table
   * @param rows list of objects that represent the rows
   */
  private Table(@Nullable String[] header, List<String[]> rows) {
    Preconditions.checkNotNull(rows);
    this.header = (header == null) ? ImmutableList.<String>of() : ImmutableList.copyOf(header);
    this.rows = ImmutableList.copyOf(rows);
  }

  public static Builder builder() {
    return new Builder();
  }

  public List<String> getHeader() {
    return header;
  }

  public List<String[]> getRows() {
    return rows;
  }

  /**
   * Builder for {@link Table}.
   */
  public static final class Builder {

    private String[] header = null;
    private List<String[]> rows = Lists.newArrayList();

    public Builder setHeader(String... header) {
      this.header = header;
      return this;
    }

    public Builder setRows(List<String[]> rows) {
      Preconditions.checkNotNull(rows);
      this.rows = rows;
      return this;
    }

    public <T> Builder setRows(List<T> rowObjects, RowMaker<T> rowMaker) {
      Preconditions.checkNotNull(rowObjects);
      Preconditions.checkNotNull(rowMaker);
      this.rows = buildRows(rowObjects, rowMaker);
      return this;
    }

    public Table build() {
      return new Table(header, rows);
    }

    private static <T> List<String[]> buildRows(List<T> records, RowMaker<T> rowMaker) {
      List<String[]> rows = Lists.newArrayList();
      for (T record : records) {
        rows.add(objectArray2StringArray(rowMaker.makeRow(record)));
      }
      return null;
    }

    private static String[] objectArray2StringArray(Object[] array) {
      String[] result = new String[array.length];
      for (int i = 0; i < array.length; i++) {
        result[i] = array[i].toString();
      }
      return result;
    }
  }
}
