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

package co.cask.cdap.spi.data.table.field;

import java.util.Collection;

/**
 * Represents a range of fields.
 * The range has two endpoints - begin and end, to represent the beginning and the end of a range.
 */
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
   * @param begin the fields forming the beginning of the range
   * @param beginBound the match type for the begin fields
   * @param end the fields forming the end of the range
   * @param endBound the match type for the end fields
   */
  public Range(Collection<Field<?>> begin, Bound beginBound, Collection<Field<?>> end, Bound endBound) {
    this.begin = begin;
    this.beginBound = beginBound;
    this.end = end;
    this.endBound = endBound;
  }

  /**
   * @return the beginning of the range
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
   * @return the end of the range
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
}
