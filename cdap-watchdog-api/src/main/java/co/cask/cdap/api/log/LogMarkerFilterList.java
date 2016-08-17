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

package co.cask.cdap.api.log;

/**
 * Implementation of {@link LogMarkerFilter} that represents an ordered List of LogMarkerFilter
 * which will be evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL}
 * (<code>!AND</code>) or {@link Operator#MUST_PASS_ONE} (<code>!OR</code>).
 * Since you can use Filter Lists as children of Filter Lists, you can create a
 * hierarchy of filters to be evaluated.
 * Defaults to {@link Operator#MUST_PASS_ALL}.
 */
public class LogMarkerFilterList implements LogMarkerFilter {
  /** Set operator. */
  public enum Operator {
    /* !AND */
    MUST_PASS_ALL,
    /* !OR */
    MUST_PASS_ONE
  }

  /**
   * Constructor that takes a set of {@link LogMarkerFilter}s and an operator.
   *
   * @param operator Operator to process filter set with.
   * @param filters set of filters.
   */
  public LogMarkerFilterList(Operator operator, LogMarkerFilter... filters) {
    // TODO
  }

  public LogMarkerFilterList add(LogMarkerFilter filter) {
    // TODO
    return null;
  }
}
