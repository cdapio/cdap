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
import io.cdap.cdap.common.metadata.QueryTerm.Comparison;
import io.cdap.cdap.common.metadata.QueryTerm.Qualifier;
import io.cdap.cdap.common.metadata.QueryTerm.SearchType;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A thread-safe class that provides helper methods for metadata search string interpretation,
 * and defines search syntax for various search term properties, i.e. the data stored in {@link QueryTerm} objects.
 */
public final class QueryParser {
  private static final Pattern SPACE_SEPARATOR_PATTERN = Pattern.compile("\\s+");
  private static final String KEYVALUE_SEPARATOR = ":";
  private static final String REQUIRED_OPERATOR = "+";

  // private constructor to prevent instantiation
  private QueryParser() {}

  /**
   * Organizes and separates a raw, space-separated search string
   * into multiple {@link QueryTerm} objects. Spaces are defined by the {@link QueryParser#SPACE_SEPARATOR_PATTERN}
   * field, the semantics of which are documented in Java's {@link Pattern} class.
   * Certain typical separations of terms, such as hyphens and commas, are not considered spaces.
   * This method preserves the original case of the query.
   *
   * QueryTerms are assigned a search type {@link QueryTerm.SearchType} based on their format. For instance,
   * if a string can be parsed as a numeric double, it will be assigned the NUMERIC type, which allows it to be used
   * in a numeric search. Search terms containing alphabetical characters and those exceeding {@link Double#MAX_VALUE}
   * will be assigned the String type.
   *
   * This method supports the use of certain search operators that, when placed before a search term,
   * denote qualifying information about that search term. When translated into a QueryTerm object, search terms
   * containing a qualifying operator have the operator removed from the string representation.
   * The {@link QueryParser#REQUIRED_OPERATOR} character signifies a search term that must receive a match.
   * By default, this method considers search items of {@link SearchType#STRING}
   * without a qualifying operator to be optional.
   * Search items of {@link SearchType#NUMERIC} are automatically required.
   *
   * For numeric searches, multiple comparison operators can be used.
   * >, >=, <, <=, or = can be placed before a numeric search field to denote a
   * greater-than, greater-than-or-equal-to, less-than, less-than-or-equal-to search, or equality search, respectively.
   * Search items without a comparison operator are considered string-based searches.
   *
   * @param query the raw search string
   * @return a list of QueryTerms
   */
  public static List<QueryTerm> parse(String query) {
    List<QueryTerm> queryTerms = new ArrayList<>();
    for (String inputTerm : Splitter.on(SPACE_SEPARATOR_PATTERN)
        .omitEmptyStrings().trimResults().split(query)) {
      queryTerms.add(parseQueryTerm(inputTerm));
    }
    return queryTerms;
  }

  /**
   * Extracts the raw value of the input term, given that terms can follow a key:[comparison-operator]value syntax.
   * This method removes any syntactic characters from the input string, including comparison and wildcard operators,
   * as well as the property qualifier, e.g. "key".
   * As an example, extractTermValue("key:>=30") returns "30".
   *
   * Note that this method removes comparison operators from alphabetic strings as well, even though they do not qualify
   * for numeric search.
   * As an example, extractTermValue("+>=thirty") returns "thirty".
   *
   * If the value consists entirely of a single operator (e.g. ">=" or "+"), that operator will be returned.
   * As an example, extractTermValue("key:>=") returns ">=", despite it typically being a comparison operator. In this
   * example, ">=" does not precede anything, and is thus considered its own search term.
   *
   * @param term the search term, with all syntactic operators included
   * @return the raw value of the search term, with all syntactic operators excluded
   */
  public static String extractTermValue(String term) {
    term = lastSubTerm(term);
    if (term.equals(">") || term.equals(">=") || term.equals("<") || term.equals("<=") || term.equals("==")) {
      return term;
    }
    if (term.startsWith(">") || term.startsWith("<") || term.startsWith("=")) {
      term = term.substring(1);
    }
    if (term.startsWith("=")) {
      term = term.substring(1);
    }
    if (term.endsWith("*")) {
      term = term.substring(0, term.length() - 1);
    }

    return term;
  }

  private static QueryTerm parseQueryTerm(String term) {
    SearchType parsedSearchType = getSearchType(term);

    Qualifier parsedQualifier = hasRequirementPrefix(term) || parsedSearchType == SearchType.NUMERIC
        ? Qualifier.REQUIRED : Qualifier.OPTIONAL;

    String parsedTerm = hasRequirementPrefix(term)
        ? term.substring(1) : term;

    Comparison parsedComparison = parsedSearchType == SearchType.STRING
        ? Comparison.EQUALS : getComparison(term);

    return new QueryTerm(parsedTerm, parsedQualifier, parsedSearchType, parsedComparison);
  }

  private static SearchType getSearchType(String term) {
    // numeric searches must begin with an explicit comparison operator
    String t = lastSubTerm(term);
    if (!(t.startsWith(">")
        || t.startsWith(">=")
        || t.startsWith("<")
        || t.startsWith("<=")
        || t.startsWith("=="))) {
      return SearchType.STRING;
    }
    try {
      Double.parseDouble(extractTermValue(t));
      return SearchType.NUMERIC;
    } catch (NumberFormatException e) {
      return SearchType.STRING;
    }
  }

  private static boolean hasRequirementPrefix(String term) {
    return (term.startsWith(REQUIRED_OPERATOR) && term.length() > 1);
  }

  private static String lastSubTerm(String term) {
    if (hasRequirementPrefix(term)) {
      term = term.substring(1);
    }
    return term.substring(term.lastIndexOf(KEYVALUE_SEPARATOR) + 1);
  }

  private static Comparison getComparison(String term) {
    term = lastSubTerm(term);
    if (term.startsWith(">=")) {
      return Comparison.GREATER_OR_EQUAL;
    }
    if (term.startsWith(">")) {
      return Comparison.GREATER;
    }
    if (term.startsWith("<=")) {
      return Comparison.LESS_OR_EQUAL;
    }
    if (term.startsWith("<")) {
      return Comparison.LESS;
    }
      return Comparison.EQUALS;
  }
}
