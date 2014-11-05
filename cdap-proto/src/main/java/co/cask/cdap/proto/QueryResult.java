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

import java.util.Arrays;
import java.util.List;

/**
 * Represents one row of a query result.
 */
public class QueryResult {

  /**
   * Type of a cell in a query result row.
   */
  public enum ResultType {
    BOOLEAN,
    BYTE,
    SHORT,
    INT,
    LONG,
    DOUBLE,
    STRING,
    BINARY,
    NULL
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
    ResultType type;
    Boolean booleanValue = null;
    Byte byteValue = null;
    Short shortValue = null;
    Integer intValue = null;
    Long longValue = null;
    Double doubleValue = null;
    String stringValue = null;
    byte[] binaryValue = null;

    public ResultObject(Boolean value) {
      this.booleanValue = value;
      this.type = ResultType.BOOLEAN;
    }

    public ResultObject(Byte value) {
      this.byteValue = value;
      this.type = ResultType.BYTE;
    }

    public ResultObject(Short shortValue) {
      this.shortValue = shortValue;
      this.type = ResultType.SHORT;
    }

    public ResultObject(Integer intValue) {
      this.intValue = intValue;
      this.type = ResultType.INT;
    }

    public ResultObject(Long longValue) {
      this.longValue = longValue;
      this.type = ResultType.LONG;
    }

    public ResultObject(Double doubleValue) {
      this.doubleValue = doubleValue;
      this.type = ResultType.DOUBLE;
    }

    public ResultObject(String stringValue) {
      this.stringValue = stringValue;
      this.type = ResultType.STRING;
    }

    public ResultObject(byte[] binaryValue) {
      this.binaryValue = binaryValue;
      this.type = ResultType.BINARY;
    }

    public ResultObject() {
      this.type = ResultType.NULL;
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

    public Object getObject() {
      switch (type) {
        case BOOLEAN:
          return booleanValue;
        case BYTE:
          return byteValue;
        case SHORT:
          return shortValue;
        case INT:
          return intValue;
        case LONG:
          return longValue;
        case DOUBLE:
          return doubleValue;
        case STRING:
          return stringValue;
        case BINARY:
          return binaryValue;
        case NULL:
          return null;
      }
      return null;
    }

    public ResultType getType() {
      return type;
    }

    @Override
    public String toString() {
      switch (type) {
        case BOOLEAN:
          return booleanValue.toString();
        case BYTE:
          return byteValue.toString();
        case SHORT:
          return shortValue.toString();
        case INT:
          return intValue.toString();
        case LONG:
          return longValue.toString();
        case DOUBLE:
          return doubleValue.toString();
        case STRING:
          return stringValue;
        case BINARY:
          return Arrays.asList(binaryValue).toString();
        case NULL:
          return "null";
      }
      return null;
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

      return Objects.equal(this.type, that.type) &&
        Objects.equal(this.booleanValue, that.booleanValue) &&
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
      return Objects.hashCode(type, booleanValue, byteValue, shortValue, intValue,
                              longValue, doubleValue, stringValue, binaryValue);
    }
  }
}
