package com.continuuity.explore.jdbc;

import com.continuuity.explore.client.ExploreClient;
import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.Result;

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
        ImmutableMap.of("foobar", (List<ColumnDesc>) Lists.newArrayList(
            new ColumnDesc("column1", "STRING", 1, ""),
            new ColumnDesc("column2", "int", 2, "")
        )),
        ImmutableMap.of("foobar", (List<Result>) Lists.<Result>newArrayList())
    );

    // Make sure an empty query still has a ResultSet associated to it
    ExploreStatement statement = new ExploreStatement(null, exploreClient);
    Assert.assertTrue(statement.execute("mock_query"));
    ResultSet rs = statement.getResultSet();
    Assert.assertNotNull(rs);
    Assert.assertFalse(rs.isClosed());
    Assert.assertFalse(rs.next());

    rs = statement.executeQuery("mock_query");
    Assert.assertNotNull(rs);
    Assert.assertFalse(rs.isClosed());
    Assert.assertFalse(rs.next());

    // Make sure subsequent calls to an execute method close the previous results
    ResultSet rs2 = statement.executeQuery("mock_query");
    Assert.assertTrue(rs.isClosed());
    Assert.assertNotNull(rs2);
    Assert.assertFalse(rs2.isClosed());
    Assert.assertFalse(rs2.next());

    Assert.assertTrue(statement.execute("mock_query"));
    Assert.assertTrue(rs2.isClosed());
  }
}
