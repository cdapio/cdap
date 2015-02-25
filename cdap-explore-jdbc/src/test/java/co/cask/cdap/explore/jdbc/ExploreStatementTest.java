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

package co.cask.cdap.explore.jdbc;

import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.List;

/**
 *
 */
public class ExploreStatementTest {

  @Test
  public void executeTest() throws Exception {
    ExploreClient exploreClient = new MockExploreClient(
        ImmutableMap.of(
          "mock_query_1", (List<ColumnDesc>) Lists.newArrayList(new ColumnDesc("column1", "STRING", 1, "")),
          "mock_query_2", (List<ColumnDesc>) Lists.newArrayList(new ColumnDesc("column1", "STRING", 1, "")),
          "mock_query_3", (List<ColumnDesc>) Lists.newArrayList(new ColumnDesc("column1", "STRING", 1, "")),
          "mock_query_4", (List<ColumnDesc>) Lists.newArrayList(new ColumnDesc("column1", "STRING", 1, ""))
        ),
        ImmutableMap.of(
          "mock_query_1", (List<QueryResult>) Lists.<QueryResult>newArrayList(),
          "mock_query_2", (List<QueryResult>) Lists.<QueryResult>newArrayList(),
          "mock_query_3", (List<QueryResult>) Lists.<QueryResult>newArrayList(),
          "mock_query_4", (List<QueryResult>) Lists.<QueryResult>newArrayList()
          )
    );

    // Make sure an empty query still has a ResultSet associated to it
    ExploreStatement statement = new ExploreStatement(null, exploreClient, "");
    Assert.assertTrue(statement.execute("mock_query_1"));
    ResultSet rs = statement.getResultSet();
    Assert.assertNotNull(rs);
    Assert.assertFalse(rs.isClosed());
    Assert.assertFalse(rs.next());

    rs = statement.executeQuery("mock_query_2");
    Assert.assertNotNull(rs);
    Assert.assertFalse(rs.isClosed());
    Assert.assertFalse(rs.next());

    // Make sure subsequent calls to an execute method close the previous results
    ResultSet rs2 = statement.executeQuery("mock_query_3");
    Assert.assertTrue(rs.isClosed());
    Assert.assertNotNull(rs2);
    Assert.assertFalse(rs2.isClosed());
    Assert.assertFalse(rs2.next());

    Assert.assertTrue(statement.execute("mock_query_4"));
    Assert.assertTrue(rs2.isClosed());
  }
}
