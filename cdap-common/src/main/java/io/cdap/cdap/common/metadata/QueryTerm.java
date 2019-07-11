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

/**
 * Represents a single item in a search query in terms of its content (i.e. the value being searched for)
 * and its qualifying information (e.g. whether a match for it is optional or required).
 * Is typically constructed in a list via {@link QueryParser#parse(String)}
 */
public class QueryTerm {
  private String term;

  /**
   * Defines the different types of search terms that can be input.
   * A qualifier determines how the search implementation should handle the given term, e.g.
   * prioritizing required terms over optional ones.
   */
  public enum Qualifier {
    OPTIONAL, REQUIRED
  }
  private Qualifier qualifier;

  public QueryTerm(String term, Qualifier qualifier) {
    this.term = term;
    this.qualifier = qualifier;
  }

  public String getTerm() {
    return term;
  }
  public Qualifier getQualifier() {
    return qualifier;
  }
  public boolean equals(QueryTerm q) {
    return term.equals(q.getTerm()) && qualifier.equals(q.getQualifier());
  }
}
