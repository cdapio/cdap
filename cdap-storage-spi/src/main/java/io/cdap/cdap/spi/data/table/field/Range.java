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

package io.cdap.cdap.spi.data.table.field;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.spi.data.InvalidFieldException;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Represents a range of fields.
 * The range has two endpoints - begin and end, to represent the beginning and the end of a range.
 */
@Beta
public final class Range {
  /**
   * Indicates if the endpoint is part of the range (INCLUSIVE) or not (EXCLUSIVE).
   */
  public enum Bound {
    INCLUSIVE,
    EXCLUSIVE
  }

  private final Collection<Field<?>> begin;
  private final Bound beginBound;
  private final Collection<Field<?>> end;
  private final Bound endBound;

  /**
   * Create a range with begin and end.
   *
   * @param begin the fields forming the beginning of the range. The fields must be a prefix or complete primary keys,
   *              empty to read from the start. If not, {@link InvalidFieldException} will be thrown
   *              when scanning the table.
   * @param beginBound the match type for the begin fields
   * @param end the fields forming the end of the range. The fields must be a prefix or complete primary keys,
   *            empty to read to the end. If not, {@link InvalidFieldException} will be thrown
   *            when scanning the table.
   * @param endBound the match type for the end fields
   */
  private Range(Collection<Field<?>> begin, Bound beginBound, Collection<Field<?>> end, Bound endBound) {
    this.begin = begin == null ? Collections.emptySet() : begin;
    this.beginBound = beginBound;
    this.end = end == null ? Collections.emptySet() : end;
    this.endBound = endBound;
  }

  /**
   * Create a range with a begin and an end.
   *
   * @param begin the fields forming the beginning of the range
   * @param beginBound the match type for the begin fields
   * @param end the fields forming the end of the range
   * @param endBound the match type for the end fields
   * @return a range object
   */
  public static Range create(Collection<Field<?>> begin, Bound beginBound, Collection<Field<?>> end, Bound endBound) {
    return new Range(begin, beginBound, end, endBound);
  }

  /**
   * Create a range that starts from a begin point, but does not have an end. It will include all values that are
   * greater than (or equal to) the begin point.
   *
   * @param begin the fields forming the beginning of the range
   * @param beginBound the match type for the begin fields
   * @return a range object
   */
  public static Range from(Collection<Field<?>> begin, Bound beginBound) {
    return new Range(begin, beginBound, Collections.emptySet(), Bound.INCLUSIVE);
  }

  /**
   * Create a range that only has an end point. It will include all values less than (or equal to) the end point.
   *
   * @param end the fields forming the end of the range
   * @param endBound the match type for the end fields
   * @return a range object
   */
  public static Range to(Collection<Field<?>> end, Bound endBound) {
    return new Range(Collections.emptySet(), Bound.INCLUSIVE, end, endBound);
  }

  /**
   * Creates a range that only matches one element. This range will read all elements which is equal to the
   * given keys.
   *
   * @param singleton the fields forming the singleton range
   * @return a range object
   */
  public static Range singleton(Collection<Field<?>> singleton) {
    return new Range(singleton, Bound.INCLUSIVE, singleton, Bound.INCLUSIVE);
  }

  /**
   * Create a range that matches all the elements.
   *
   * @return a range object
   */
  public static Range all() {
    return new Range(Collections.emptySet(), Bound.INCLUSIVE, Collections.emptySet(), Bound.INCLUSIVE);
  }

  /**
   * Returns {@code true} if the range is a single value range.
   */
  public boolean isSingleton() {
    return !begin.isEmpty() && begin.equals(end) && beginBound == Bound.INCLUSIVE && endBound == Bound.INCLUSIVE;
  }

  /**
   * @return the beginning of the range, if empty, it will be reading from the start
   */
  public Collection<Field<?>> getBegin() {
    return begin;
  }

  /**
   * @return the match type of the begin fields
   */
  public Bound getBeginBound() {
    return beginBound;
  }

  /**
   * @return the end of the range, if empty, it will be reading till the end
   */
  public Collection<Field<?>> getEnd() {
    return end;
  }

  /**
   * @return the match type of the end fields
   */
  public Bound getEndBound() {
    return endBound;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    Range range = (Range) other;
    return Objects.equals(begin, range.begin) && beginBound == range.beginBound
      && Objects.equals(end, range.end) && endBound == range.endBound;
  }

  @Override
  public int hashCode() {
    return Objects.hash(begin, beginBound, end, endBound);
  }

  @Override
  public String toString() {
    return "Range{" +
      "begin=" + begin +
      ", beginBound=" + beginBound +
      ", end=" + end +
      ", endBound=" + endBound +
      '}';
  }
}
