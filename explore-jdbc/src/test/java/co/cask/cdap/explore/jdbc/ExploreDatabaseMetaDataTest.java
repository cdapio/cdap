/*
 * Copyright 2014 Cask, Inc.
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 *
 */
public class ExploreDatabaseMetaDataTest {

  @Test
  public void getMetadataEndpointsTest() throws Exception {
    MetadataInfoCalls.assertResultSet(new MetadataInfoCalls("tableTypes_stmt").getMetadata().getTableTypes());
    MetadataInfoCalls.assertResultSet(new MetadataInfoCalls("columns_stmt").getMetadata()
                                        .getColumns(null, null, "%", "%"));
    MetadataInfoCalls.assertResultSet(new MetadataInfoCalls("dataTypes_stmt").getMetadata().getTypeInfo());
    MetadataInfoCalls.assertResultSet(new MetadataInfoCalls("tables_stmt").getMetadata()
                                        .getTables(null, null, "%", null));
    MetadataInfoCalls.assertResultSet(new MetadataInfoCalls("functions_stmt").getMetadata()
                                        .getFunctions(null, null, "%"));
    MetadataInfoCalls.assertResultSet(new MetadataInfoCalls("schemas_stmt").getMetadata().getSchemas());
    MetadataInfoCalls.assertResultSet(new MetadataInfoCalls("schemas_stmt").getMetadata().getSchemas(null, null));
    MetadataInfoCalls.assertResultSet(new MetadataInfoCalls("catalogs_stmt").getMetadata().getCatalogs());
  }

  @Test
  public void getImportedKeysTest() throws Exception {
    ResultSet results = new ExploreDatabaseMetaData(null, null).getImportedKeys("foo", "bar", "foobar");
    Assert.assertFalse(results.next());
    Assert.assertEquals(14, results.getMetaData().getColumnCount());
    Assert.assertEquals("PKTABLE_CAT", results.getMetaData().getColumnName(1));
    Assert.assertEquals("PKTABLE_SCHEM", results.getMetaData().getColumnName(2));
    Assert.assertEquals("PKTABLE_NAME", results.getMetaData().getColumnName(3));
    Assert.assertEquals("PKCOLUMN_NAME", results.getMetaData().getColumnName(4));
    Assert.assertEquals("FKTABLE_CAT", results.getMetaData().getColumnName(5));
    Assert.assertEquals("FKTABLE_SCHEM", results.getMetaData().getColumnName(6));
    Assert.assertEquals("FKTABLE_NAME", results.getMetaData().getColumnName(7));
    Assert.assertEquals("FKCOLUMN_NAME", results.getMetaData().getColumnName(8));
    Assert.assertEquals("KEY_SEQ", results.getMetaData().getColumnName(9));
    Assert.assertEquals("UPDATE_RULE", results.getMetaData().getColumnName(10));
    Assert.assertEquals("DELETE_RULE", results.getMetaData().getColumnName(11));
    Assert.assertEquals("FK_NAME", results.getMetaData().getColumnName(12));
    Assert.assertEquals("PK_NAME", results.getMetaData().getColumnName(13));
    Assert.assertEquals("DEFERRABILITY", results.getMetaData().getColumnName(14));

    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(1));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(2));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(3));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(4));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(5));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(6));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(7));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(8));
    Assert.assertEquals("smallint", results.getMetaData().getColumnTypeName(9));
    Assert.assertEquals("smallint", results.getMetaData().getColumnTypeName(10));
    Assert.assertEquals("smallint", results.getMetaData().getColumnTypeName(11));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(12));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(13));
    Assert.assertEquals("string", results.getMetaData().getColumnTypeName(14));
  }

  @Test
  public void getPrimaryKeysTest() throws Exception {
    ResultSet results = new ExploreDatabaseMetaData(null, null).getPrimaryKeys("foo", "bar", "foobar");
    Assert.assertFalse(results.next());
    Assert.assertEquals(6, results.getMetaData().getColumnCount());
  }

  @Test
  public void getProcedureColumnsTest() throws Exception {
    ResultSet results = new ExploreDatabaseMetaData(null, null).getProcedureColumns("foo", "bar", "foobar", "foo2");
    Assert.assertFalse(results.next());
    Assert.assertEquals(20, results.getMetaData().getColumnCount());
  }

  @Test
  public void getProceduresTest() throws Exception {
    ResultSet results = new ExploreDatabaseMetaData(null, null).getProcedures("foo", "bar", "foobar");
    Assert.assertFalse(results.next());
    Assert.assertEquals(9, results.getMetaData().getColumnCount());
  }

  @Test
  public void getUDTsTest() throws Exception {
    ResultSet results = new ExploreDatabaseMetaData(null, null).getUDTs("foo", "bar", "foobar", null);
    Assert.assertFalse(results.next());
    Assert.assertEquals(7, results.getMetaData().getColumnCount());
  }

  private static final class MetadataInfoCalls {
    private DatabaseMetaData metadata;

    MetadataInfoCalls(String handle) {
      init(handle);
    }

    private void init(String statement) {
      ExploreClient exploreClient = new MockExploreClient(
        ImmutableMap.of(statement, (List<ColumnDesc>) Lists.newArrayList(
                          new ColumnDesc("column1", "STRING", 1, ""),
                          new ColumnDesc("column2", "INT", 2, ""))
        ),
        ImmutableMap.of(statement, (List<QueryResult>) Lists.newArrayList(
          new QueryResult(ImmutableList.<Object>of("some value", 10))
        ))
      );

      metadata = new ExploreDatabaseMetaData(null, exploreClient);
    }

    public DatabaseMetaData getMetadata() {
      return metadata;
    }

    public static void assertResultSet(ResultSet results) throws SQLException {
      Assert.assertTrue(results.next());
      Assert.assertEquals("some value", results.getString(1));
      Assert.assertEquals(10, results.getInt(2));
      Assert.assertFalse(results.next());
      results.close();
    }
  }
}
