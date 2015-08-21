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
import java.sql.SQLException;
import java.util.List;

/**
 *
 */
public class ExplorePreparedStatementTest {

  @Test
  public void executeTest() throws Exception {
    ExploreClient exploreClient = new MockExploreClient(
      ImmutableMap.of("SELECT * FROM table WHERE id=100, name='foo'", (List<ColumnDesc>) Lists.newArrayList(
        new ColumnDesc("column1", "STRING", 1, ""),
        new ColumnDesc("column2", "int", 2, ""))
      ),
      ImmutableMap.of("SELECT * FROM table WHERE id=100, name='foo'",
                      (List<QueryResult>) Lists.<QueryResult>newArrayList())
    );


    ExplorePreparedStatement statement = new ExplorePreparedStatement(null, exploreClient,
                                                                      "SELECT * FROM table WHERE id=?, name=?", "");
    statement.setInt(1, 100);
    try {
      statement.execute();
      Assert.fail();
    } catch (SQLException e) {
      // Parameter 2 has not been set
    }

    statement.setString(2, "foo");
    Assert.assertEquals("SELECT * FROM table WHERE id=100, name='foo'", statement.updateSql());

    Assert.assertTrue(statement.execute());
    ResultSet rs = statement.getResultSet();
    Assert.assertNotNull(rs);
    Assert.assertFalse(rs.isClosed());
    Assert.assertFalse(rs.next());

    statement = new ExplorePreparedStatement(null, exploreClient, "SELECT * FROM table WHERE name='?'", "");
    Assert.assertEquals("SELECT * FROM table WHERE name='?'", statement.updateSql());

    statement = new ExplorePreparedStatement(null, exploreClient, "SELECT * FROM table WHERE name='?', id=?", "");
    statement.setInt(1, 100);
    Assert.assertEquals("SELECT * FROM table WHERE name='?', id=100", statement.updateSql());

    statement = new ExplorePreparedStatement(null, exploreClient, "SELECT * FROM table WHERE name=\"?\", id=?", "");
    statement.setInt(1, 100);
    Assert.assertEquals("SELECT * FROM table WHERE name=\"?\", id=100", statement.updateSql());

    statement = new ExplorePreparedStatement(null, exploreClient, "SELECT * FROM table WHERE name=\"'?'\", id=?", "");
    statement.setInt(1, 100);
    Assert.assertEquals("SELECT * FROM table WHERE name=\"'?'\", id=100", statement.updateSql());

    statement = new ExplorePreparedStatement(null, exploreClient, "SELECT * FROM table WHERE name=\"'?\", id=?", "");
    statement.setInt(1, 100);
    Assert.assertEquals("SELECT * FROM table WHERE name=\"'?\", id=100", statement.updateSql());

    statement = new ExplorePreparedStatement(null, exploreClient,
                                             "SELECT * FROM table WHERE name=\"\\\"?\\\"\", id=?", "");
    statement.setInt(1, 100);
    Assert.assertEquals("SELECT * FROM table WHERE name=\"\\\"?\\\"\", id=100", statement.updateSql());
  }
}
