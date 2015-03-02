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

package co.cask.cdap.proto;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Represents query result.
 */
public class QueryResult {
  private final List<Object> columns;

  public QueryResult(List<Object> columns) {
    this.columns = ImmutableList.copyOf(columns);
  }

  public List<Object> getColumns() {
    return columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryResult that = (QueryResult) o;
    if (this.columns.size() != that.columns.size()) {
      return false;
    }
    // Handle byte[] equality
    Iterator<Object> thisIte = this.columns.iterator();
    Iterator<Object> thatIte = that.columns.iterator();
    while (thisIte.hasNext() && thatIte.hasNext()) {
      Object thisCol = thisIte.next();
      Object thatCol = thatIte.next();
      if (thisCol instanceof byte[] && thatCol instanceof byte[]) {
        if (!Arrays.equals((byte[]) thisCol, (byte[]) thatCol)) {
          return false;
        }
      } else if (!Objects.equal(thisCol, thatCol)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(columns);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("columns", columns)
      .toString();
  }
}
