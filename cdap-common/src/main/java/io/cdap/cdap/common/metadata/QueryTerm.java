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
 * and any useful properties of the search term, e.g. its qualifier and search type.
 * Is typically constructed in a list via {@link QueryParser#parse(String)}
 */
public class QueryTerm {
  private final String term;
  private final Qualifier qualifier;
  private final SearchType searchType;
  private final Comparison comparison;

  /**
   * Defines the different types of search operators that can be used.
   * A qualifier determines how the search implementation should prioritize the given term, e.g.
   * prioritizing required terms over optional ones.
   */
  public enum Qualifier {
    OPTIONAL, REQUIRED
  }

  /**
   * Defines the different types of search terms that can be used.
   * A search type describes the intuitive object type of the term;
   * for instance, the term may be intuited as a number and parsed as one, though internally represented as a String.
   * Its search type would be considered NUMERIC.
   */
  public enum SearchType {
    STRING, NUMERIC
  }

  /**
   * Defines the different relationships a search term can have to potential matches.
   * For a String or keyword search, only EQUALS is valid.
   */
  public enum Comparison {
    EQUALS, GREATER, GREATER_OR_EQUAL, LESS, LESS_OR_EQUAL
  }

  /**
   * Older constructor that assumes a simple String search. Ineligible for numeric search fields.
   *
   * @param term the search term
   * @param qualifier the qualifying information {@link Qualifier}
   */
  public QueryTerm(String term, Qualifier qualifier) {
    this(term, qualifier, SearchType.STRING, Comparison.EQUALS);
  }
  /**
   * Constructs a QueryTerm using the search term, qualifying information, search type, and comparison type.
   *
   * @param term the search term
   * @param qualifier the qualifying information {@link Qualifier}
   * @param searchType the intuitive object type {@link SearchType}
   * @param comparison the desired relative value of potential matches {@link Comparison}
   */
  public QueryTerm(String term, Qualifier qualifier, SearchType searchType, Comparison comparison) {
    this.term = term;
    this.qualifier = qualifier;
    this.searchType = searchType;
    this.comparison = comparison;
  }

  public String getTerm() {
    return term;
  }

  public Qualifier getQualifier() {
    return qualifier;
  }

  public SearchType getSearchType() {
    return searchType;
  }

  public Comparison getComparison() {
    return comparison;
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

    return Objects.equals(term, that.getTerm())
        && Objects.equals(qualifier, that.getQualifier())
        && Objects.equals(searchType, that.getSearchType())
        && Objects.equals(comparison, that.getComparison());
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, qualifier, searchType, comparison);
  }

  @Override
  public String toString() {
    return "term:" + term
        + ", qualifier: " + qualifier
        + ", searchType: " + searchType
        + ", comparison: " + comparison;
  }
}
