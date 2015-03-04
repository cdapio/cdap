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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 *
 */
public class ExploreResultSetTest {
  @Test
  public void testResultSet() throws Exception {
    ExploreClient exploreClient = new MockExploreClient(
        ImmutableMap.of("mock_query", (List<ColumnDesc>) Lists.newArrayList(
            new ColumnDesc("column1", "STRING", 1, ""),
            new ColumnDesc("column2", "int", 2, ""),
            new ColumnDesc("column3", "char", 3, ""),
            new ColumnDesc("column4", "float", 4, ""),
            new ColumnDesc("column5", "double", 5, ""),
            new ColumnDesc("column6", "boolean", 6, ""),
            new ColumnDesc("column7", "tinyint", 7, ""),
            new ColumnDesc("column8", "smallint", 8, ""),
            new ColumnDesc("column9", "bigint", 9, ""),
            new ColumnDesc("column10", "date", 10, ""),
            new ColumnDesc("column11", "timestamp", 11, ""),
            new ColumnDesc("column12", "decimal", 12, ""),
            new ColumnDesc("column14", "map<string,string>", 13, ""),
            new ColumnDesc("column15", "array<string>", 14, ""),
            new ColumnDesc("column16", "struct<name:string,attr:string>", 15, "")
        )),
        ImmutableMap.of("mock_query", (List<QueryResult>) Lists.newArrayList(
            new QueryResult(ImmutableList.<Object>of(
                "value1",
                1,
                "c",
                0.1f,
                0.2d,
                true,
                0x1,
                (short) 2,
                (long) 10,
                "2014-06-20",
                "2014-06-20 07:37:00",
                "1000000000",
                "\"{\"key1\":\"value1\"}",
                "[\"a\",\"b\",\"c\"]",
                "{\"name\":\"first\",\"attr\":\"second\"}"
            ))
        ))
    );

    ResultSet resultSet = new ExploreResultSet(exploreClient.submit(Id.Namespace.from(""), "mock_query").get(),
                                               new ExploreStatement(null, exploreClient, ""),
                                               0);
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(resultSet.getObject(1), resultSet.getObject("column1"));
    Assert.assertEquals("value1", resultSet.getString(1));
    Assert.assertEquals(1, resultSet.getInt(2));
    Assert.assertEquals("c", resultSet.getString(3));
    Assert.assertEquals(0.1f, resultSet.getFloat(4), 0.01);
    Assert.assertEquals(0.2d, resultSet.getDouble(5), 0.01);
    Assert.assertEquals(true, resultSet.getBoolean(6));
    Assert.assertEquals(0x1, resultSet.getByte(7));
    Assert.assertEquals(2, resultSet.getShort(8));
    Assert.assertEquals(10, resultSet.getLong(9));
    Assert.assertEquals(Date.valueOf("2014-06-20"), resultSet.getDate(10));
    Assert.assertEquals(Timestamp.valueOf("2014-06-20 07:37:00"), resultSet.getTimestamp(11));
    Assert.assertEquals(new BigDecimal("1000000000"), resultSet.getBigDecimal(12));
    Assert.assertEquals("\"{\"key1\":\"value1\"}", resultSet.getString(13));
    Assert.assertEquals("[\"a\",\"b\",\"c\"]", resultSet.getString(14));
    Assert.assertEquals("{\"name\":\"first\",\"attr\":\"second\"}", resultSet.getString(15));
    Assert.assertFalse(resultSet.next());

    Assert.assertFalse(resultSet.next());
    try {
      resultSet.getObject(1);
    } catch (SQLException e) {
      // Expected: no more rows
    }
  }

  @Test
  public void sameNamedColumns() throws Exception {
    ExploreClient exploreClient = new MockExploreClient(
        ImmutableMap.of("mock_query", (List<ColumnDesc>) Lists.newArrayList(
            new ColumnDesc("column1", "STRING", 2, ""),
            new ColumnDesc("column1", "int", 1, "")
        )),
        ImmutableMap.of("mock_query", (List<QueryResult>) Lists.newArrayList(
            new QueryResult(ImmutableList.<Object>of(1, "value1"))
        ))
    );

    ResultSet resultSet = new ExploreResultSet(exploreClient.submit(Id.Namespace.from(""), "mock_query").get(),
                                               new ExploreStatement(null, exploreClient, ""),
                                               0);
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(1, resultSet.findColumn("column1"));
    Assert.assertEquals(1, resultSet.getObject("column1"));

    // Can't call get... after resultSet is closed, nor findColumn
    resultSet.close();
    try {
      resultSet.getObject(1);
    } catch (SQLException e) {
      // Expected
    }
    try {
      resultSet.findColumn("column1");
    } catch (SQLException e) {
      // Expected
    }
  }
}
