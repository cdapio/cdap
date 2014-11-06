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
    Boolean booleanValue = null;
    Byte byteValue = null;
    Short shortValue = null;
    Integer intValue = null;
    Long longValue = null;
    Double doubleValue = null;
    String stringValue = null;
    byte[] binaryValue = null;

    /**
     * Create a Hive query result row cell based on a value.
     */
    public static ResultObject of(Object value) {
      if (value == null) {
        return of();
      }
      return new ResultObject(value);
    }

    /**
     * Create a null Hive query result row cell.
     */
    public static ResultObject of() {
      return new ResultObject(null);
    }

    private ResultObject(Object value) {
      switch (ResultType.of(value.getClass())) {
        case BOOLEAN:
          booleanValue = (Boolean) value;
          break;
        case BYTE:
          byteValue = (Byte) value;
          break;
        case SHORT:
          shortValue = (Short) value;
          break;
        case INT:
          intValue = (Integer) value;
          break;
        case LONG:
          longValue = (Long) value;
          break;
        case DOUBLE:
          doubleValue = (Double) value;
          break;
        case STRING:
          stringValue = (String) value;
          break;
        case BINARY:
          binaryValue = (byte[]) value;
          break;
        case NULL:
          break;
      }
    }

    public Boolean getBooleanValue() {
      return booleanValue;
    }

    public Byte getByteValue() {
      return byteValue;
    }

    public Short getShortValue() {
      return shortValue;
    }

    public Integer getIntValue() {
      return intValue;
    }

    public Long getLongValue() {
      return longValue;
    }

    public Double getDoubleValue() {
      return doubleValue;
    }

    public String getStringValue() {
      return stringValue;
    }

    public byte[] getBinaryValue() {
      return binaryValue;
    }

    public ResultType getType() {
      return ResultType.of(getObject().getClass());
    }

    public Object getObject() {
      if (booleanValue != null) {
        return booleanValue;
      } else if (byteValue != null) {
        return byteValue;
      } else if (shortValue != null) {
        return shortValue;
      } else if (intValue != null) {
        return intValue;
      } else if (longValue != null) {
        return longValue;
      } else if (doubleValue != null) {
        return doubleValue;
      } else if (stringValue != null) {
        return stringValue;
      } else if (binaryValue != null) {
        return binaryValue;
      }
      return null;
    }

    @Override
    public String toString() {
      Object object = getObject();
      if (object == null) {
        return "null";
      }
      return object.toString();
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
      return Objects.equal(this.booleanValue, that.booleanValue) &&
        Objects.equal(this.byteValue, that.byteValue) &&
        Objects.equal(this.shortValue, that.shortValue) &&
        Objects.equal(this.intValue, that.intValue) &&
        Objects.equal(this.longValue, that.longValue) &&
        Objects.equal(this.doubleValue, that.doubleValue) &&
        Objects.equal(this.stringValue, that.stringValue) &&
        Arrays.equals(this.binaryValue, that.binaryValue);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(booleanValue, byteValue, shortValue, intValue,
                              longValue, doubleValue, stringValue, binaryValue);
    }
  }
}
