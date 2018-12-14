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

package co.cask.cdap.data2.spi.table.field;

import java.util.Collection;

/**
 * Represents a range of fields.
 * The range has two endpoints - begin and end, to represent the beginning and the end of a range.
 */
public class Range {
  /**
   * Indicates if the endpoint is part of the range (INCLUSIVE) or not (EXCLUSIVE).
   */
  public enum Bounds {
    INCLUSIVE,
    EXCLUSIVE
  }

  private final Collection<Field<?>> begin;
  private final Bounds beginBound;
  private final Collection<Field<?>> end;
  private final Bounds endBound;

  private Range(Collection<Field<?>> begin, Bounds beginBound, Collection<Field<?>> end, Bounds endBound) {
    this.begin = begin;
    this.beginBound = beginBound;
    this.end = end;
    this.endBound = endBound;
  }

  public Collection<Field<?>> getBegin() {
    return begin;
  }

  public Bounds getBeginBound() {
    return beginBound;
  }

  public Collection<Field<?>> getEnd() {
    return end;
  }

  public Bounds getEndBound() {
    return endBound;
  }
}
