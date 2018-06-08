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

package co.cask.cdap.report.proto;

import co.cask.cdap.report.util.ReportField;

import java.util.ArrayList;
import javax.annotation.Nullable;

/**
 * Represents a filter that checks whether a given value of a field is within the allowed range.
 *
 * @param <T> type of the values
 */
public class RangeFilter<T extends Comparable<T>> extends Filter<T> {
  private final Range<T> range;

  public RangeFilter(String fieldName, Range<T> range) {
    super(fieldName);
    this.range = range;
  }

  /**
   * @return the allowed range of values of this field
   */
  public Range getRange() {
    return range;
  }

  /**
   * @return {@code true} if the given value is larger or equal to min if min is not {@code null}
   *         and smaller than max if max is not {@code null}, {@code false} otherwise
   */
  @Override
  public boolean apply(T value) {
    return (range.getMin() == null || range.getMin().compareTo(value) <= 0)
      && (range.getMax() == null || range.getMax().compareTo(value) > 0);
  }

  @Override
  @Nullable
  public String getError() {
    ArrayList<String> errors = new ArrayList<>();
    String fieldError = super.getError();
    if (fieldError != null) {
      errors.add(fieldError);
    }
    errors.addAll(getFilterTypeErrors(ReportField.FilterType.RANGE));
    if (range == null) {
      errors.add("'range' cannot be null");
    } else {
      if (range.getMin() == null && range.getMax() == null) {
        errors.add("'min' and 'max' cannot both be null'");
      } else if (range.getMin() != null && range.getMax() != null && range.getMin().compareTo(range.getMax()) >= 0) {
        errors.add("'min' must be smaller than 'max'");
      }
    }
    return errors.isEmpty() ? null :
      String.format("Filter %s contains these errors: %s", getFieldName(), String.join("; ", errors));
  }

  @Override
  public String toString() {
    return "RangeFilter{" +
      "fieldName=" + getFieldName() +
      ", range=" + range +
      '}';
  }

  /**
   * Range of allowed values of a field represented as [min, max) where min is the inclusive minimum value
   * and max is the exclusive maximum value.
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

    /**
     * @return the inclusive minimum value of this range
     */
    @Nullable
    public T getMin() {
      return min;
    }

    /**
     * @return the exclusive maximum value of this range
     */
    @Nullable
    public T getMax() {
      return max;
    }

    @Override
    public String toString() {
      return "Range{" +
        "min=" + min +
        ", max=" + max +
        '}';
    }
  }
}
