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

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.common.metadata.QueryTerm.Comparison;
import io.cdap.cdap.common.metadata.QueryTerm.Qualifier;
import io.cdap.cdap.common.metadata.QueryTerm.SearchType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class QueryParserTest {
  @Test
  public void testRequiredOperatorIsDetected() {
    String inputQuery = "tag1 tag2 +tag3 tag4+";
    List<QueryTerm> targetOutputQueryTerms = ImmutableList.of(
        new QueryTerm("tag1", Qualifier.OPTIONAL),
        new QueryTerm("tag2", Qualifier.OPTIONAL),
        new QueryTerm("tag3", Qualifier.REQUIRED),
        new QueryTerm("tag4+", Qualifier.OPTIONAL));
    List<QueryTerm> actualOutputQueryTerms = QueryParser.parse(inputQuery);

    Assert.assertEquals(targetOutputQueryTerms, actualOutputQueryTerms);
  }

  @Test
  public void testWhitespaceIsTrimmed() {
    String inputQuery = "tag1       tag2      tag3    tag4";
    List<QueryTerm> outputQueryTerms = QueryParser.parse(inputQuery);

    Assert.assertEquals(4, outputQueryTerms.size());
    for (QueryTerm q : outputQueryTerms) {
      Assert.assertEquals(4, q.getTerm().length());
    }
  }

  @Test
  public void testEmptyQuery() {
    Assert.assertTrue(QueryParser.parse("").isEmpty());
  }

  @Test
  public void testWhitespacePlusValidTerm() {
    Assert.assertEquals(QueryParser.parse("        space").get(0),
        new QueryTerm("space", Qualifier.OPTIONAL));
  }

  @Test
  public void testRequiredOperatorAsSearchTerm() {
    Assert.assertEquals(QueryParser.parse("+").get(0),
        new QueryTerm("+", Qualifier.OPTIONAL));
  }

  @Test
  public void testRequiredOperatorAsRequiredSearchTerm() {
    Assert.assertEquals(QueryParser.parse("++").get(0),
        new QueryTerm("+", Qualifier.REQUIRED));
  }

  @Test
  public void testAlternativeWhitespaceCharactersAreValid() {
    String tabSeparatedString = "tag1\ttag2";
    String newlineSeparatedString = "tag1\ntag2";
    String formFeedSeparatedString = "tag1\ftag2";
    String carriageReturnSeparatedString = "tag\rtag2";

    Assert.assertEquals(2, QueryParser.parse(tabSeparatedString).size());
    Assert.assertEquals(2, QueryParser.parse(newlineSeparatedString).size());
    Assert.assertEquals(2, QueryParser.parse(formFeedSeparatedString).size());
    Assert.assertEquals(2, QueryParser.parse(carriageReturnSeparatedString).size());
  }

  @Test
  public void testBareNumberIsString() {
    Assert.assertEquals(new QueryTerm("1", Qualifier.OPTIONAL, SearchType.STRING, Comparison.EQUALS),
        QueryParser.parse("1").get(0));
  }

  @Test
  public void testKeyValueIsNumeric() {
    Assert.assertEquals(new QueryTerm("key:==10", Qualifier.REQUIRED, SearchType.NUMERIC, Comparison.EQUALS),
        QueryParser.parse("key:==10").get(0));
    Assert.assertEquals(new QueryTerm("key:==10.0", Qualifier.REQUIRED, SearchType.NUMERIC, Comparison.EQUALS),
        QueryParser.parse("key:==10.0").get(0));
    Assert.assertEquals(new QueryTerm("key:==ten", Qualifier.OPTIONAL, SearchType.STRING, Comparison.EQUALS),
        QueryParser.parse("key:==ten").get(0));
  }

  @Test
  public void testComparisonOperatorsAreSupported() {
    Assert.assertEquals(new QueryTerm("key:>10", Qualifier.REQUIRED, SearchType.NUMERIC, Comparison.GREATER),
        QueryParser.parse("key:>10").get(0));
    Assert.assertEquals(new QueryTerm("key:>=10", Qualifier.REQUIRED, SearchType.NUMERIC, Comparison.GREATER_OR_EQUAL),
        QueryParser.parse("key:>=10").get(0));
    Assert.assertEquals(new QueryTerm("key:<10", Qualifier.REQUIRED, SearchType.NUMERIC, Comparison.LESS),
        QueryParser.parse("key:<10").get(0));
    Assert.assertEquals(new QueryTerm("key:<=10", Qualifier.REQUIRED, SearchType.NUMERIC, Comparison.LESS_OR_EQUAL),
        QueryParser.parse("key:<=10").get(0));
  }

  @Test
  public void testExtractTermValueOfNumbers() {
    Assert.assertEquals("30", QueryParser.extractTermValue("key:>=30"));
    Assert.assertEquals("30.0", QueryParser.extractTermValue("key:>=30.0"));
    Assert.assertEquals("30", QueryParser.extractTermValue(">=30"));
    Assert.assertEquals("30", QueryParser.extractTermValue("30"));
    Assert.assertEquals("=30", QueryParser.extractTermValue("key:>==30"));
  }

  @Test
  public void testExtractTermValueOfStrings() {
    Assert.assertEquals("thirty", QueryParser.extractTermValue("key:>=thirty"));
    Assert.assertEquals("thirty", QueryParser.extractTermValue("+thirty"));
    Assert.assertEquals("thirty", QueryParser.extractTermValue("+>=thirty"));
    Assert.assertEquals("+", QueryParser.extractTermValue("+"));
  }

  @Test
  public void testExtractTermValueOfComparisonOperator() {
    Assert.assertEquals(">=", QueryParser.extractTermValue(">="));
    Assert.assertEquals(">=", QueryParser.extractTermValue("+>="));
    Assert.assertEquals(">=", QueryParser.extractTermValue(">>="));
    Assert.assertEquals("+", QueryParser.extractTermValue(">+"));
    Assert.assertEquals(">", QueryParser.extractTermValue("+key:>"));
  }

  @Test
  public void testDateParser() {
    String dateString = "2019-08-05";
    Assert.assertEquals(Long.valueOf(1564963200000L), QueryParser.parseDate(dateString));
  }

  @Test
  public void testParseDateQuery() {
    String query = "DATE:creation_time:<2019-08-05";
    QueryTerm expected = new QueryTerm("creation_time:<2019-08-05", QueryTerm.Qualifier.REQUIRED,
                                       QueryTerm.SearchType.DATE, QueryTerm.Comparison.LESS,
                                       Long.valueOf(1564963200000L));
    QueryTerm actual = QueryParser.parse(query).get(0);
    Assert.assertEquals(expected, actual);
  }
}
