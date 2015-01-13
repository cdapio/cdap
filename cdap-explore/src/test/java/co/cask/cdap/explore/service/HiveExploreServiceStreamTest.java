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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 *
 */
@Category(SlowTests.class)
public class HiveExploreServiceStreamTest extends BaseHiveExploreServiceTest {
  private static final Gson GSON = new Gson();
  private static final String body1 = "userX,actionA,item123";
  private static final String body2 = "userY,actionB,item123";
  private static final String body3 = "userZ,actionA,item456";
  private static final String streamName = "mystream";
  private static final String streamTableName = "cdap_stream_" + streamName;
  // headers must be prefixed with the stream name, otherwise they are filtered out.
  private static final Map<String, String> headers = ImmutableMap.of("header1", "val1", "header2", "val2");
  private static final Type headerType = new TypeToken<Map<String, String>>() { }.getType();

  @BeforeClass
  public static void start() throws Exception {
    // use leveldb implementations, since stream input format examines the filesystem
    // to determine input splits.
    startServices(CConfiguration.create(), true);

    createStream(streamName);
    sendStreamEvent(streamName, headers, Bytes.toBytes(body1));
    sendStreamEvent(streamName, headers, Bytes.toBytes(body2));
    sendStreamEvent(streamName, headers, Bytes.toBytes(body3));
  }

  @Test
  public void testStreamDefaultSchema() throws Exception {
    runCommand("describe " + streamTableName,
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("ts", "bigint", "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("headers", "map<string,string>",
                                                            "from deserializer")),
                 new QueryResult(Lists.<Object>newArrayList("body", "string", "from deserializer"))
               )
    );
  }

  @Test
  public void testSelectStarOnStream() throws Exception {
    ExploreExecutionResult results = exploreClient.submit("select * from " + streamTableName).get();
    // check schema
    List<ColumnDesc> expectedSchema = Lists.newArrayList(
      new ColumnDesc(streamTableName + ".ts", "BIGINT", 1, null),
      new ColumnDesc(streamTableName + ".headers", "map<string,string>", 2, null),
      new ColumnDesc(streamTableName + ".body", "STRING", 3, null)
    );
    Assert.assertEquals(expectedSchema, results.getResultSchema());
    // check each result, without checking timestamp since that changes for each test
    // first result
    List<Object> columns = results.next().getColumns();
    // maps are returned as json objects...
    Assert.assertEquals(headers, GSON.fromJson((String) columns.get(1), headerType));
    Assert.assertEquals(body1, columns.get(2));
    // second result
    columns = results.next().getColumns();
    Assert.assertEquals(headers, GSON.fromJson((String) columns.get(1), headerType));
    Assert.assertEquals(body2, columns.get(2));
    // third result
    columns = results.next().getColumns();
    Assert.assertEquals(headers, GSON.fromJson((String) columns.get(1), headerType));
    Assert.assertEquals(body3, columns.get(2));
    // should not be any more
    Assert.assertFalse(results.hasNext());
  }

  @Test
  public void testSelectFieldOnStream() throws Exception {
    runCommand("select body from " + streamTableName,
               true,
               Lists.newArrayList(new ColumnDesc("body", "STRING", 1, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList(body1)),
                 new QueryResult(Lists.<Object>newArrayList(body2)),
                 new QueryResult(Lists.<Object>newArrayList(body3)))
    );

    runCommand("select headers[\"header1\"] as h1, headers[\"header2\"] as h2 from " + streamTableName,
               true,
               Lists.newArrayList(new ColumnDesc("h1", "STRING", 1, null),
                                  new ColumnDesc("h2", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("val1", "val2")),
                 new QueryResult(Lists.<Object>newArrayList("val1", "val2")),
                 new QueryResult(Lists.<Object>newArrayList("val1", "val2")))
    );
  }

  @Test
  public void testSelectAndFilterQueryOnStream() throws Exception {
    runCommand("select body from " + streamTableName + " where ts > " + Long.MAX_VALUE,
               false,
               Lists.newArrayList(new ColumnDesc("body", "STRING", 1, null)),
               Lists.<QueryResult>newArrayList());
  }

  @Test
  public void testJoinOnStreams() throws Exception {
    createStream("jointest1");
    createStream("jointest2");
    sendStreamEvent("jointest1", Collections.<String, String>emptyMap(), Bytes.toBytes("ABC"));
    sendStreamEvent("jointest1", Collections.<String, String>emptyMap(), Bytes.toBytes("XYZ"));
    sendStreamEvent("jointest2", Collections.<String, String>emptyMap(), Bytes.toBytes("ABC"));
    sendStreamEvent("jointest2", Collections.<String, String>emptyMap(), Bytes.toBytes("DEF"));

    runCommand("select cdap_stream_jointest1.body, cdap_stream_jointest2.body" +
                 " from cdap_stream_jointest1 join cdap_stream_jointest2" +
                 " on (cdap_stream_jointest1.body = cdap_stream_jointest2.body)",
               true,
               Lists.newArrayList(new ColumnDesc("cdap_stream_jointest1.body", "STRING", 1, null),
                                  new ColumnDesc("cdap_stream_jointest2.body", "STRING", 2, null)),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList("ABC", "ABC")))
    );
  }

  @Test(expected = ExecutionException.class)
  public void testWriteToStreamFails() throws Exception {
    exploreClient.submit("insert into table " + streamTableName + " select * from " + streamTableName).get();
  }
}
