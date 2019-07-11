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

<<<<<<< HEAD
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.common.metadata.QueryTerm.Qualifier;
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
=======
import io.cdap.cdap.common.metadata.QueryTerm.Qualifier;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class QueryParserTest {
  @Test
  public void testBasicParse() {
    String inputQuery = "tag1 tag2 +tag3 tag4+";
    List<QueryTerm> targetOutputQueryTerms = new ArrayList<>();
    targetOutputQueryTerms.add(new QueryTerm("tag1", Qualifier.OPTIONAL));
    targetOutputQueryTerms.add(new QueryTerm("tag2", Qualifier.OPTIONAL));
    targetOutputQueryTerms.add(new QueryTerm("tag3", Qualifier.REQUIRED));
    targetOutputQueryTerms.add(new QueryTerm("tag4+", Qualifier.OPTIONAL));
    List<QueryTerm> actualOutputQueryTerms = QueryParser.parse(inputQuery);

    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(targetOutputQueryTerms.get(i).equals(actualOutputQueryTerms.get(i)));
    }
    Assert.assertFalse(targetOutputQueryTerms.get(1).equals(actualOutputQueryTerms.get(2)));
  }

  @Test
  public void testFormatting() {
    String inputQuery = "tag1       tag2      tag3    tag4";
    List<QueryTerm> outputQueryTerms = QueryParser.parse(inputQuery);

    Assert.assertEquals(outputQueryTerms.size(), 4);
    for (int i = 0; i < 4; i++) {
      Assert.assertEquals(QueryParser.parse(inputQuery).get(i).getTerm().length(), 4);
>>>>>>> Added basic QueryParser functionality and test cases
    }
  }

  @Test
  public void testUnusualQueries() {
<<<<<<< HEAD
    String emptyQuery = "";
    String whitespacePlusValidTerm = "        space";
    String operatorAsSearchTerm = "+";
    String operatorAsRequiredSearchTerm = "++";

    Assert.assertTrue(QueryParser.parse(emptyQuery).isEmpty());
    Assert.assertEquals(QueryParser.parse(whitespacePlusValidTerm).get(0), new QueryTerm("space", Qualifier.OPTIONAL));
    Assert.assertEquals(QueryParser.parse(operatorAsSearchTerm).get(0), new QueryTerm("+", Qualifier.OPTIONAL));
    Assert.assertEquals(QueryParser.parse(operatorAsRequiredSearchTerm).get(0), new QueryTerm("+", Qualifier.REQUIRED));
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
}
=======
    String inputQuery1 = "";
    String inputQuery2 = "        space";
    String inputQuery3 = "+ plus";

    Assert.assertTrue(QueryParser.parse(inputQuery1).isEmpty());
    Assert.assertTrue(QueryParser.parse(inputQuery2).get(0).equals(new QueryTerm("space", Qualifier.OPTIONAL)));
    Assert.assertTrue(QueryParser.parse(inputQuery3).get(0).equals(new QueryTerm("", Qualifier.REQUIRED)));
    Assert.assertTrue(QueryParser.parse(inputQuery3).get(1).equals(new QueryTerm("plus", Qualifier.OPTIONAL)));
  }
}
>>>>>>> Added basic QueryParser functionality and test cases
