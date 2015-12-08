/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Test for {@link StreamViewClient}.
 */
@Category(XSlowTests.class)
public class StreamViewClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamViewClientTestRun.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private StreamViewClient streamViewClient;
  private StreamClient streamClient;
  private QueryClient queryClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    streamViewClient = new StreamViewClient(clientConfig);
    streamClient = new StreamClient(clientConfig);
    queryClient = new QueryClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    Id.Namespace namespace = Id.Namespace.DEFAULT;
    Id.Stream stream = Id.Stream.from(namespace, "foo");
    Id.Stream.View view1 = Id.Stream.View.from(stream, "view1");
    LOG.info("Creating stream {}", stream);
    streamClient.create(stream);

    try {
      LOG.info("Sending events to stream {}", stream);
      streamClient.sendEvent(stream, "a,b,c");
      streamClient.sendEvent(stream, "d,e,f");
      streamClient.sendEvent(stream, "g,h,i");

      LOG.info("Verifying that no views exist yet");
      Assert.assertEquals(ImmutableList.of(), streamViewClient.list(stream));
      try {
        streamViewClient.get(view1);
        Assert.fail();
      } catch (NotFoundException e) {
        Assert.assertEquals(view1, e.getObject());
      }

      FormatSpecification format = new FormatSpecification(
        "csv",
        Schema.recordOf(
          "foo",
          Schema.Field.of("one", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("two", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("three", Schema.of(Schema.Type.STRING))));
      ViewSpecification viewSpecification = new ViewSpecification(format, "firsttable");
      LOG.info("Creating view {} with config {}", view1, GSON.toJson(viewSpecification));
      Assert.assertEquals(true, streamViewClient.createOrUpdate(view1, viewSpecification));

      LOG.info("Verifying that view {} has been created", view1);
      Assert.assertEquals(new ViewDetail(view1.getId(), viewSpecification), streamViewClient.get(view1));
      Assert.assertEquals(ImmutableList.of(view1.getId()), streamViewClient.list(stream));

      FormatSpecification newFormat = new FormatSpecification(
        "csv",
        Schema.recordOf(
          "foo",
          Schema.Field.of("one", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("two", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("three", Schema.of(Schema.Type.STRING))));
      ViewSpecification newViewSpecification = new ViewSpecification(newFormat, "sometable");
      LOG.info("Updating view {} with config {}", view1, GSON.toJson(newViewSpecification));
      Assert.assertEquals(false, streamViewClient.createOrUpdate(view1, newViewSpecification));

      LOG.info("Verifying that view {} has been updated", view1);
      Assert.assertEquals(new ViewDetail(view1.getId(), newViewSpecification), streamViewClient.get(view1));
      Assert.assertEquals(ImmutableList.of(view1.getId()), streamViewClient.list(stream));

      ExploreExecutionResult executionResult = queryClient.execute(
        view1.getNamespace(), "select one,two,three from sometable").get();
      Assert.assertNotNull(executionResult.getResultSchema());
      Assert.assertEquals(3, executionResult.getResultSchema().size());
      Assert.assertEquals("one", executionResult.getResultSchema().get(0).getName());
      Assert.assertEquals("two", executionResult.getResultSchema().get(1).getName());
      Assert.assertEquals("three", executionResult.getResultSchema().get(2).getName());

      List<QueryResult> results = Lists.newArrayList(executionResult);
      Assert.assertNotNull(results);
      Assert.assertEquals(3, results.size());
      Assert.assertEquals("a", results.get(0).getColumns().get(0));
      Assert.assertEquals("b", results.get(0).getColumns().get(1));
      Assert.assertEquals("c", results.get(0).getColumns().get(2));
      Assert.assertEquals("d", results.get(1).getColumns().get(0));
      Assert.assertEquals("e", results.get(1).getColumns().get(1));
      Assert.assertEquals("f", results.get(1).getColumns().get(2));
      Assert.assertEquals("g", results.get(2).getColumns().get(0));
      Assert.assertEquals("h", results.get(2).getColumns().get(1));
      Assert.assertEquals("i", results.get(2).getColumns().get(2));

      LOG.info("Deleting view {}", view1);
      streamViewClient.delete(view1);

      LOG.info("Verifying that view {] has been deleted", view1);
      try {
        streamViewClient.get(view1);
        Assert.fail();
      } catch (NotFoundException e) {
        Assert.assertEquals(view1, e.getObject());
      }
      Assert.assertEquals(ImmutableList.of(), streamViewClient.list(stream));
    } finally {
      streamClient.delete(stream);
    }

    // test deleting stream with a view
    LOG.info("Creating stream {}", stream);
    streamClient.create(stream);

    try {
      FormatSpecification format = new FormatSpecification(
        "csv",
        Schema.recordOf(
          "foo",
          Schema.Field.of("one", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("two", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("three", Schema.of(Schema.Type.STRING))));
      ViewSpecification viewSpecification = new ViewSpecification(format, "firsttable");
      LOG.info("Creating view {} with config {}", view1, GSON.toJson(viewSpecification));
      Assert.assertEquals(true, streamViewClient.createOrUpdate(view1, viewSpecification));

      streamClient.delete(stream);
    } finally {
      streamViewClient.delete(view1);
      streamClient.delete(stream);
    }
  }
}
