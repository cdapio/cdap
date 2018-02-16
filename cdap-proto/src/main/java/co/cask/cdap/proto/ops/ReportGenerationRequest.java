/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.ops;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Represents a request to process a program run report in an HTTP request.
 */
public class ReportGenerationRequest {
  private final long start;
  private final long end;
  private final List<String> fields;
  private final List<Sort> sort;
  private final List<Filter> filters;

  public ReportGenerationRequest(long start, long end, List<String> fields, List<Sort> sort, List<Filter> filters) {
    this.start = start;
    this.end = end;
    this.fields = fields;
    this.sort = sort;
    this.filters = filters;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public List<String> getFields() {
    return fields;
  }

  public List<Sort> getSort() {
    return sort;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  /**
   * Represents a flied in the report.
   */
  public static class Field {
    private final String fieldName;

    public Field(String fieldName) {
      this.fieldName = fieldName;
    }

    public String getFieldName() {
      return fieldName;
    }
  }

  /**
   * A class represents the field to sort the report by and the order of sorting by this field.
   */
  public static class Sort extends Field {
    private final Order order;

    public Sort(String fieldName, Order order) {
      super(fieldName);
      this.order = order;
    }

    public Order getOrder() {
      return order;
    }
  }

  /**
   *
   */
  public static class Filter extends Field {
    public Filter(String fieldName) {
      super(fieldName);
    }
  }

  /**
   * Allowed values in a field to be included in the report.
   *
   * @param <T> type of the values
   */
  public static class ValueFilter<T> extends Filter {
    @Nullable
    private final List<T> whitelist;
    @Nullable
    private final List<T> blacklist;

    public ValueFilter(String fieldName, @Nullable List<T> whitelist, @Nullable List<T> blacklist) {
      super(fieldName);
      this.whitelist = whitelist;
      this.blacklist = blacklist;
    }

    @Nullable
    public List<T> getWhitelist() {
      return whitelist;
    }

    @Nullable
    public List<T> getBlacklist() {
      return blacklist;
    }
  }

  /**
   * Allowed range of values in a field to be included in the report.
   *
   * @param <T> the value type of the field
   */
  public static class RangeFilter<T> extends Filter {
    private final Range range;

    public RangeFilter(String fieldName, Range<T> range) {
      super(fieldName);
      this.range = range;
    }

    public Range getRange() {
      return range;
    }
  }

  /**
   * Range of values.
   *
   * @param <T> the value type of the field
   */
  public static class Range<T> {
    @Nullable
    private final T min;
    @Nullable
    private final T max;

    public Range(T min, T max) {
      this.min = min;
      this.max = max;
    }

    @Nullable
    public T getMin() {
      return min;
    }

    @Nullable
    public T getMax() {
      return max;
    }
  }

  /**
   * The order to sort a field by.
   */
  public enum Order {
    ASCENDING,
    DESCENDING
  }
}
