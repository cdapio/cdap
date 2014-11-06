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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Represents one row of a query result.
 */
public class QueryResult {

  /**
   * Type of a cell in a query result row.
   */
  public enum ResultType {
    BOOLEAN(Boolean.class),
    BYTE(Byte.class),
    SHORT(Short.class),
    INT(Integer.class),
    LONG(Long.class),
    DOUBLE(Double.class),
    STRING(String.class),
    BINARY(byte[].class),
    NULL(null);

    private static Map<Class<?>, ResultType> types;
    private final Class<?> cls;

    static {
      types = Maps.newIdentityHashMap();
      for (ResultType type : ResultType.values()) {
        if (type.cls != null) {
          types.put(type.cls, type);
        }
      }
    }

    private ResultType(Class<?> cls) {
      this.cls = cls;
    }

    static ResultType of(Class<?> cls) {
      ResultType type = types.get(cls);
      Preconditions.checkArgument(type != null, String.format("Type %s is not supported.", cls));
      return type;
    }
  }

  private final List<ResultObject> columns;

  public QueryResult(List<ResultObject> columns) {
    this.columns = columns;
  }

  public List<ResultObject> getColumns() {
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

    return Objects.equal(this.columns, that.columns);
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

  /**
   * Represents one cell in a query result row.
   */
  public static class ResultObject {
    ResultType type = null;
    Object value = null;

    /**
     * Create a Hive query result row cell based on a value.
     */
    public static ResultObject of(Object value) {
      if (value == null) {
        return of();
      }
      return new ResultObject(ResultType.of(value.getClass()), value);
    }

    /**
     * Create a null Hive query result row cell.
     */
    public static ResultObject of() {
      return new ResultObject(ResultType.NULL, null);
    }

    private ResultObject(ResultType type, Object value) {
      this.type = type;
      this.value = value;
    }

    public ResultType getType() {
      return type;
    }

    public Object getValue() {
      return value;
    }

    @Override
    public String toString() {
      if (value == null) {
        return "null";
      }
      return value.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ResultObject that = (ResultObject) o;
      if (this.value instanceof byte[] && that.value instanceof byte[]) {
        return Objects.equal(this.type, that.type) &&
          Arrays.equals((byte[]) this.value, (byte[]) that.value);
      }
      return Objects.equal(this.type, that.type) &&
        Objects.equal(this.value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(type, value);
    }
  }
}
