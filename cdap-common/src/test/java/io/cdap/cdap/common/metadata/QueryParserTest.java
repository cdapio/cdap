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
    }
  }

  @Test
  public void testUnusualQueries() {
    String inputQuery1 = "";
    String inputQuery2 = "        space";
    String inputQuery3 = "+ plus";

    Assert.assertTrue(QueryParser.parse(inputQuery1).isEmpty());
    Assert.assertTrue(QueryParser.parse(inputQuery2).get(0).equals(new QueryTerm("space", Qualifier.OPTIONAL)));
    Assert.assertTrue(QueryParser.parse(inputQuery3).get(0).equals(new QueryTerm("", Qualifier.REQUIRED)));
    Assert.assertTrue(QueryParser.parse(inputQuery3).get(1).equals(new QueryTerm("plus", Qualifier.OPTIONAL)));
  }
}