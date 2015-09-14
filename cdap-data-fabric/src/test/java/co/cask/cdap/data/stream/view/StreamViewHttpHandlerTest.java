/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.stream.view;

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.stream.StreamViewHttpHandler;
import co.cask.cdap.data.view.InMemoryViewStore;
import co.cask.cdap.data.view.ViewAdmin;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.explore.client.MockExploreClient;
import co.cask.cdap.explore.utils.ExploreTableNaming;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.ViewDetail;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.common.http.ObjectResponse;
import co.cask.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class StreamViewHttpHandlerTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private NettyHttpService service;
  private URI baseURI;

  @Before
  public void setUp() {
    InMemoryViewStore store = new InMemoryViewStore();
    ExploreFacade explore = new ExploreFacade(new MockExploreClient(), CConfiguration.create());
    StreamViewHttpHandler handler = new StreamViewHttpHandler(
      new ViewAdmin(store, explore, new ExploreTableNaming()));

    service = NettyHttpService.builder()
      .addHttpHandlers(ImmutableList.of(handler))
      .setExceptionHandler(new HttpExceptionHandler())
      .build();
    service.startAndWait();
    InetSocketAddress bindAddress = service.getBindAddress();
    baseURI = URI.create("http://" + bindAddress.getHostName() + ":" + bindAddress.getPort());
  }

  @After
  public void tearDown() {
    service.stopAndWait();
  }

  @Test
  public void testAll() throws Exception {
    List<String> views = execute(
      200, HttpRequest.get(resolve("/v3/namespaces/default/streams/foo/views")).build(),
      new TypeToken<List<String>>() {
      }.getType());
    Assert.assertEquals(ImmutableList.of(), views);

    Schema schema = Schema.recordOf("foo", Schema.Field.of("name", Schema.of(Schema.Type.STRING)));
    FormatSpecification formatSpec = new FormatSpecification(
      Formats.AVRO, schema, Collections.<String, String>emptyMap());
    ViewSpecification config = new ViewSpecification(formatSpec);

    execute(
      201,
      HttpRequest.put(resolve("/v3/namespaces/default/streams/foo/views/view1"))
        .withBody(GSON.toJson(config))
        .build());
    ViewDetail actualDetail = execute(
      200,
      HttpRequest.get(resolve("/v3/namespaces/default/streams/foo/views/view1")).build(),
      ViewDetail.class);
    Assert.assertEquals(
      new ViewDetail("view1", new ViewSpecification(config.getFormat(), "stream_foo_view1")), actualDetail);

    views = execute(
      200, HttpRequest.get(resolve("/v3/namespaces/default/streams/foo/views")).build(),
      new TypeToken<List<String>>() {
      }.getType());
    Assert.assertEquals(ImmutableList.of("view1"), views);

    execute(
      201,
      HttpRequest.put(resolve("/v3/namespaces/default/streams/foo/views/view2"))
        .withBody(GSON.toJson(config))
        .build());
    actualDetail = execute(
      200,
      HttpRequest.get(resolve("/v3/namespaces/default/streams/foo/views/view2")).build(),
      ViewDetail.class);
    Assert.assertEquals(
      new ViewDetail("view2", new ViewSpecification(config.getFormat(), "stream_foo_view2")), actualDetail);

    views = execute(
      200, HttpRequest.get(resolve("/v3/namespaces/default/streams/foo/views")).build(),
      new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(ImmutableList.of("view1", "view2"),
                        views == null ? null : Ordering.natural().sortedCopy(views));

    execute(200, HttpRequest.delete(resolve("/v3/namespaces/default/streams/foo/views/view1")).build());
    views = execute(
      200, HttpRequest.get(resolve("/v3/namespaces/default/streams/foo/views")).build(),
      new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(ImmutableList.of("view2"), views);

    execute(200, HttpRequest.delete(resolve("/v3/namespaces/default/streams/foo/views/view2")).build());
    views = execute(
      200, HttpRequest.get(resolve("/v3/namespaces/default/streams/foo/views")).build(),
      new TypeToken<List<String>>() { }.getType());
    Assert.assertEquals(ImmutableList.of(), views);
  }

  private URL resolve(String format, Object... args) throws MalformedURLException {
    return baseURI.resolve(String.format(format, args)).toURL();
  }

  private <T> T execute(int expectedCode, HttpRequest request, Type type) throws IOException {
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(response.getResponseMessage(), expectedCode, response.getResponseCode());

    if (type != null) {
      try {
        return ObjectResponse.<T>fromJsonBody(response, type, GSON).getResponseObject();
      } catch (JsonSyntaxException e) {
        throw new RuntimeException("Couldn't decode body as JSON: " + response.getResponseBodyAsString(), e);
      }
    }

    return null;
  }

  private void execute(int expectedCode, HttpRequest request) throws IOException {
    execute(expectedCode, request, null);
  }

}
