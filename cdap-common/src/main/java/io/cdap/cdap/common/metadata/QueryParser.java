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

import com.google.common.base.Splitter;
import io.cdap.cdap.common.metadata.QueryTerm.Qualifier;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A thread-safe class that provides helper methods for metadata search string interpretation,
 * and defines search syntax for qualifying information (e.g. required terms) {@link QueryTerm.Qualifier}.
 */
public final class QueryParser {
  private static final Pattern SPACE_SEPARATOR_PATTERN = Pattern.compile("\\s+");
  private static final char REQUIRED_OPERATOR = '+';

  // private constructor to prevent instantiation
  private QueryParser() {}

  /**
   * Organizes and separates a raw, space-separated search string
   * into multiple {@link QueryTerm} objects. Spaces are defined by the {@link QueryParser#SPACE_SEPARATOR_PATTERN}
   * field, the semantics of which are documented in Java's {@link Pattern} class.
   * Certain typical separations of terms, such as hyphens and commas, are not considered spaces.
   * This method preserves the original case of the query.
   *
   * This method supports the use of certain search operators that, when placed before a search term,
   * denote qualifying information about that search term. When translated into a QueryTerm object, search terms
   * containing an operator have the operator removed from the string representation.
   * The {@link QueryParser#REQUIRED_OPERATOR} character signifies a search term that must receive a match.
   * By default, this method considers search items without an operator to be optional.
   *
   * @param query the raw search string
   * @return a list of QueryTerms
   */
  public static List<QueryTerm> parse(String query) {
    List<QueryTerm> queryTerms = new ArrayList<>();
    for (String term : Splitter.on(SPACE_SEPARATOR_PATTERN)
        .omitEmptyStrings().trimResults().split(query)) {
      queryTerms.add(parseQueryTerm(term));
    }
    return queryTerms;
  }

  private static QueryTerm parseQueryTerm(String term) {
    if (term.charAt(0) == REQUIRED_OPERATOR && term.length() > 1) {
      return new QueryTerm(term.substring(1), Qualifier.REQUIRED);
    }
      return new QueryTerm(term, Qualifier.OPTIONAL);
  }
}
