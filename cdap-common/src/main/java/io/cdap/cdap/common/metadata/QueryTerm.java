/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.metadata;

import java.util.Objects;

/**
 * Represents a single item in a search query in terms of its content (i.e. the value being searched for)
 * and its qualifying information (e.g. whether a match for it is optional or required).
 * Is typically constructed in a list via {@link QueryParser#parse(String)}
 */
public class QueryTerm {
  private final String term;
  private final Qualifier qualifier;

  /**
   * Defines the different types of search terms that can be input.
   * A qualifier determines how the search implementation should handle the given term, e.g.
   * prioritizing required terms over optional ones.
   */
  public enum Qualifier {
    OPTIONAL, REQUIRED
  }

  /**
   * Constructs a QueryTerm using the search term and its qualifying information.
   *
   * @param term the search term
   * @param qualifier the qualifying information {@link Qualifier}
   */
  public QueryTerm(String term, Qualifier qualifier) {
    this.term = term;
    this.qualifier = qualifier;
  }

  /**
   * @return the search term, without its preceding operator
   */
  public String getTerm() {
    return term;
  }

  /**
   * @return the search term's qualifying information
   */
  public Qualifier getQualifier() {
    return qualifier;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryTerm that = (QueryTerm) o;

    return Objects.equals(term, that.getTerm()) && Objects.equals(qualifier, that.getQualifier());
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, qualifier);
  }
}
