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
import com.google.common.base.Function;
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
  private final Iterable<List<String>> rows;

  /**
   * @param header strings representing the header of the table
   * @param rows list of objects that represent the rows
   */
  private Table(@Nullable List<String> header, Iterable<List<String>> rows) {
    Preconditions.checkNotNull(rows);
    this.header = (header == null) ? ImmutableList.<String>of() : ImmutableList.copyOf(header);
    this.rows = rows;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder.Rows rows() {
    return new Builder.Rows();
  }


  public List<String> getHeader() {
    return header;
  }

  public Iterable<List<String>> getRows() {
    return rows;
  }

  /**
   * Builder for {@link Table}.
   */
  public static final class Builder {

    private List<String> header = null;
    private Iterable<List<String>> rows = null;

    public Builder setHeader(String... header) {
      this.header = ImmutableList.copyOf(header);
      return this;
    }

    public Builder setRows(Iterable<List<String>> rows) {
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

    private static <T> List<List<String>> buildRows(List<T> records, final RowMaker<T> rowMaker) {
      return Lists.transform(records, new Function<T, List<String>>() {
        @Nullable
        @Override
        public List<String> apply(@Nullable T input) {
          return objectArray2StringArray(rowMaker.makeRow(input));
        }
      });
    }

    private static List<String> objectArray2StringArray(List<?> array) {
      return Lists.transform(array, new Function<Object, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Object input) {
          if (input == null) {
            return null;
          }
          return input.toString();
        }
      });
    }

    /**
     * Builder for {@link Table#rows}.
     */
    public static final class Rows {
      private List<List<String>> rows = Lists.newArrayList();

      public Rows add(String... row) {
        rows.add(ImmutableList.copyOf(row));
        return this;
      }

      public Iterable<List<String>> build() {
        return rows;
      }
    }

  }
}
