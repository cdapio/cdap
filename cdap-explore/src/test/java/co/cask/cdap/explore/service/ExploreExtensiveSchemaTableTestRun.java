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

package co.cask.cdap.explore.service;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.explore.service.datasets.ExtensiveSchemaTableDefinition;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.Transaction;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Painfully test a wide combination of types in a schema of a record scannable.
 */
@Category(SlowTests.class)
public class ExploreExtensiveSchemaTableTestRun extends BaseHiveExploreServiceTest {

  private static final Id.DatasetModule extensiveSchema = Id.DatasetModule.from(NAMESPACE_ID, "extensiveSchema");

  @BeforeClass
  public static void start() throws Exception {
    startServices();

    datasetFramework.addModule(extensiveSchema, new ExtensiveSchemaTableDefinition.ExtensiveSchemaTableModule());

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("ExtensiveSchemaTable", MY_TABLE, DatasetProperties.EMPTY);

    // Accessing dataset instance to perform data operations
    ExtensiveSchemaTableDefinition.ExtensiveSchemaTable table =
      datasetFramework.getDataset(MY_TABLE, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(table);

    Transaction tx1 = transactionManager.startShort(100);
    table.startTx(tx1);

    ExtensiveSchemaTableDefinition.ExtensiveSchema value1 =
      new ExtensiveSchemaTableDefinition.ExtensiveSchema(
        "foo", 1, 1.23f, 2.45d, (long) 1000, (byte) 100, true, (short) 8, new int[] { 10, 11 },
        new float[] { 1.67f, 2.89f }, new double[] { 10.56d, 8.78d }, new long [] { 101, 201 },
        new byte[] { 106, 110 }, new boolean[] { true, false }, new short[] { 50, 51 }, new String[] { "foo", "bar" },
        ImmutableList.of(10, 20), ImmutableList.of(10.45f, 20.98f), ImmutableList.of(10.99d, 20.90d),
        ImmutableList.of((long) 654, (long) 2897), ImmutableList.of((byte) 22, (byte) 23), ImmutableList.of(true, true),
        ImmutableList.of((short) 76, (short) 39), ImmutableList.of("foo2", "bar2"), ImmutableMap.of("foo3", 51),
        ImmutableMap.of(3.55f, 51.98d), ImmutableMap.of((long) 890, (byte) 45), ImmutableMap.of(true, (short) 27),
        new ExtensiveSchemaTableDefinition.Value("foo", 2),
        new ExtensiveSchemaTableDefinition.Value[] {
          new ExtensiveSchemaTableDefinition.Value("bar", 3), new ExtensiveSchemaTableDefinition.Value("foobar", 4)
        }, ImmutableList.of(new ExtensiveSchemaTableDefinition.Value("foobar2", 3)),
        ImmutableMap.of("key", new ExtensiveSchemaTableDefinition.Value("foobar3", 9)));
    value1.setExt(value1);
    table.put("1", value1);

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);
  }

  @AfterClass
  public static void stop() throws Exception {
    datasetFramework.deleteInstance(MY_TABLE);
    datasetFramework.deleteModule(extensiveSchema);
  }

  @Test
  public void testExtensiveSchema() throws Exception {
    runCommand("show tables",
               true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList("my_table"))));

    runCommand("describe my_table",
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("s", "string", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("i", "int", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("f", "float", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("d", "double", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("l", "bigint", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("b", "tinyint", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("bo", "boolean", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("sh", "smallint", "from deserializer")),
                 // note: hive removes upper cases
                 // Arrays
                 new QueryResult(Lists.<Object>newArrayList("iarr", "array<int>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("farr", "array<float>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("darr", "array<double>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("larr", "array<bigint>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("barr", "binary", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("boarr", "array<boolean>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("sharr", "array<smallint>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("sarr", "array<string>", "from deserializer")),
                 // Lists
                 new QueryResult(Lists.<Object>newArrayList("ilist", "array<int>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("flist", "array<float>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("dlist", "array<double>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("llist", "array<bigint>", "from deserializer")),
                 // Note: list<byte> becomes array<tinyint>, whereas byte[] becomes binary...
                 new QueryResult(Lists.<Object>newArrayList("blist", "array<tinyint>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("bolist", "array<boolean>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("shlist", "array<smallint>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("slist", "array<string>", "from deserializer")),
                 // Maps
                 new QueryResult(Lists.<Object>newArrayList("stoimap", "map<string,int>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("ftodmap", "map<float,double>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("ltobmap", "map<bigint,tinyint>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("botoshmap", "map<boolean,smallint>", "from deserializer")),
                 // Custom type
                 new QueryResult(Lists.<Object>newArrayList("v", "struct<s:string,i:int>", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("varr", "array<struct<s:string,i:int>>",
                                                            "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("vlist", "array<struct<s:string,i:int>>",
                                                            "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("stovmap", "map<string,struct<s:string,i:int>>",
                                                       "from deserializer"))
               )
    );

    runCommand("select * from my_table",
               true,
               Lists.newArrayList(new ColumnDesc("my_table.s", "STRING", 1, null),
                                  new ColumnDesc("my_table.i", "INT", 2, null),
                                  new ColumnDesc("my_table.f", "FLOAT", 3, null),
                                  new ColumnDesc("my_table.d", "DOUBLE", 4, null),
                                  new ColumnDesc("my_table.l", "BIGINT", 5, null),
                                  new ColumnDesc("my_table.b", "TINYINT", 6, null),
                                  new ColumnDesc("my_table.bo", "BOOLEAN", 7, null),
                                  new ColumnDesc("my_table.sh", "SMALLINT", 8, null),
                                  // Arrays
                                  new ColumnDesc("my_table.iarr", "array<int>", 9, null),
                                  new ColumnDesc("my_table.farr", "array<float>", 10, null),
                                  new ColumnDesc("my_table.darr", "array<double>", 11, null),
                                  new ColumnDesc("my_table.larr", "array<bigint>", 12, null),
                                  new ColumnDesc("my_table.barr", "BINARY", 13, null),
                                  new ColumnDesc("my_table.boarr", "array<boolean>", 14, null),
                                  new ColumnDesc("my_table.sharr", "array<smallint>", 15, null),
                                  new ColumnDesc("my_table.sarr", "array<string>", 16, null),
                                  // Lists
                                  new ColumnDesc("my_table.ilist", "array<int>", 17, null),
                                  new ColumnDesc("my_table.flist", "array<float>", 18, null),
                                  new ColumnDesc("my_table.dlist", "array<double>", 19, null),
                                  new ColumnDesc("my_table.llist", "array<bigint>", 20, null),
                                  new ColumnDesc("my_table.blist", "array<tinyint>", 21, null),
                                  new ColumnDesc("my_table.bolist", "array<boolean>", 22, null),
                                  new ColumnDesc("my_table.shlist", "array<smallint>", 23, null),
                                  new ColumnDesc("my_table.slist", "array<string>", 24, null),
                                  // Maps
                                  new ColumnDesc("my_table.stoimap", "map<string,int>", 25, null),
                                  new ColumnDesc("my_table.ftodmap", "map<float,double>", 26, null),
                                  new ColumnDesc("my_table.ltobmap", "map<bigint,tinyint>", 27, null),
                                  new ColumnDesc("my_table.botoshmap",
                                                 "map<boolean,smallint>", 28, null),
                                  // Custom type
                                  new ColumnDesc("my_table.v", "struct<s:string,i:int>", 29, null),
                                  new ColumnDesc("my_table.varr", "array<struct<s:string,i:int>>",
                                                 30, null),
                                  new ColumnDesc("my_table.vlist", "array<struct<s:string,i:int>>",
                                                 31, null),
                                  new ColumnDesc("my_table.stovmap",
                                                 "map<string,struct<s:string,i:int>>", 32, null)
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList(
                   // scalars
                   "foo", 1, 1.23, 2.45, (long) 1000, (byte) 100, true, (short) 8,
                   // arrays
                   "[10,11]",
                   "[1.67,2.89]",
                   "[10.56,8.78]",
                   "[101,201]",
                   new byte[] { 106, 110 },
                   "[true,false]",
                   "[50,51]",
                   "[\"foo\",\"bar\"]",
                   // lists
                   "[10,20]",
                   "[10.45,20.98]",
                   "[10.99,20.9]",
                   "[654,2897]",
                   new byte[] { 22, 23 },
                   "[true,true]",
                   "[76,39]",
                   "[\"foo2\",\"bar2\"]",
                   // maps
                   "{\"foo3\":51}", "{3.55:51.98}", "{890:45}",
                   "{true:27}", "{\"s\":\"foo\",\"i\":2}", "[{\"s\":\"bar\",\"i\":3},{\"s\":\"foobar\",\"i\":4}]",
                   "[{\"s\":\"foobar2\",\"i\":3}]", "{\"key\":{\"s\":\"foobar3\",\"i\":9}}"
                 )))
    );

    // Make sure the whole schema has been persisted in the metastore, and with the right order
    Assert.assertEquals(ImmutableList.of(new TableInfo.ColumnInfo("s", "string", null),
                                         new TableInfo.ColumnInfo("i", "int", null),
                                         new TableInfo.ColumnInfo("f", "float", null),
                                         new TableInfo.ColumnInfo("d", "double", null),
                                         new TableInfo.ColumnInfo("l", "bigint", null),
                                         new TableInfo.ColumnInfo("b", "tinyint", null),
                                         new TableInfo.ColumnInfo("bo", "boolean", null),
                                         new TableInfo.ColumnInfo("sh", "smallint", null),
                                         // Arrays
                                         new TableInfo.ColumnInfo("iarr", "array<int>", null),
                                         new TableInfo.ColumnInfo("farr", "array<float>", null),
                                         new TableInfo.ColumnInfo("darr", "array<double>", null),
                                         new TableInfo.ColumnInfo("larr", "array<bigint>", null),
                                         new TableInfo.ColumnInfo("barr", "binary", null),
                                         new TableInfo.ColumnInfo("boarr", "array<boolean>", null),
                                         new TableInfo.ColumnInfo("sharr", "array<smallint>", null),
                                         new TableInfo.ColumnInfo("sarr", "array<string>", null),
                                         // Lists
                                         new TableInfo.ColumnInfo("ilist", "array<int>", null),
                                         new TableInfo.ColumnInfo("flist", "array<float>", null),
                                         new TableInfo.ColumnInfo("dlist", "array<double>", null),
                                         new TableInfo.ColumnInfo("llist", "array<bigint>", null),
                                         new TableInfo.ColumnInfo("blist", "array<tinyint>", null),
                                         new TableInfo.ColumnInfo("bolist", "array<boolean>", null),
                                         new TableInfo.ColumnInfo("shlist", "array<smallint>", null),
                                         new TableInfo.ColumnInfo("slist", "array<string>", null),
                                         // Maps
                                         new TableInfo.ColumnInfo("stoimap", "map<string,int>", null),
                                         new TableInfo.ColumnInfo("ftodmap", "map<float,double>", null),
                                         new TableInfo.ColumnInfo("ltobmap", "map<bigint,tinyint>", null),
                                         new TableInfo.ColumnInfo("botoshmap", "map<boolean,smallint>", null),
                                         // Custom type
                                         new TableInfo.ColumnInfo("v", "struct<s:string,i:int>", null),
                                         new TableInfo.ColumnInfo("varr", "array<struct<s:string,i:int>>", null),
                                         new TableInfo.ColumnInfo("vlist", "array<struct<s:string,i:int>>", null),
                                         new TableInfo.ColumnInfo("stovmap", "map<string,struct<s:string,i:int>>", null)
                        ),
                        exploreService.getTableInfo("default", "my_table").getSchema());
  }
}
