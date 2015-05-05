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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
public class ExploreDriverTest {

  private static String exploreServiceUrl;
  private static MockHttpService httpService;

  @BeforeClass
  public static void start() throws Exception {
    httpService = new MockHttpService(new MockExploreExecutorHandler());
    httpService.startAndWait();

    Class.forName("co.cask.cdap.explore.jdbc.ExploreDriver");
    exploreServiceUrl = String.format("%s%s:%d", Constants.Explore.Jdbc.URL_PREFIX, "localhost", httpService.getPort());
    exploreServiceUrl += "?namespace=testNamespace";
  }

  @AfterClass
  public static void stop() throws Exception {
    httpService.stopAndWait();
  }

  @Test
  public void parseConnectionUrlTest() throws Exception {
    ExploreDriver driver = new ExploreDriver();

    String baseUrl = String.format("%s%s:%d", Constants.Explore.Jdbc.URL_PREFIX, "foobar", 10000);
    ExploreDriver.ConnectionParams connectionParams;

    connectionParams = driver.parseConnectionUrl(baseUrl);
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(ImmutableMultimap.of(), connectionParams.getExtraInfos());

    connectionParams = driver.parseConnectionUrl(baseUrl + "?auth.token=foo");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreDriver.ConnectionParams.Info.EXPLORE_AUTH_TOKEN, "foo"),
      connectionParams.getExtraInfos());

    connectionParams = driver.parseConnectionUrl(baseUrl + "?auth.token=foo&foo2=bar2");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreDriver.ConnectionParams.Info.EXPLORE_AUTH_TOKEN, "foo"),
      connectionParams.getExtraInfos());

    connectionParams = driver.parseConnectionUrl(baseUrl + "?foo2=bar2&auth.token=foo");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreDriver.ConnectionParams.Info.EXPLORE_AUTH_TOKEN, "foo"),
      connectionParams.getExtraInfos());

    connectionParams = driver.parseConnectionUrl(baseUrl + "?foo2=bar2&auth.token");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(ImmutableMultimap.of(), connectionParams.getExtraInfos());

    connectionParams = driver.parseConnectionUrl(baseUrl + "?foo2=bar2&auth.token=foo,bar");
    Assert.assertEquals("foobar", connectionParams.getHost());
    Assert.assertEquals(10000, connectionParams.getPort());
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreDriver.ConnectionParams.Info.EXPLORE_AUTH_TOKEN, "foo",
                           ExploreDriver.ConnectionParams.Info.EXPLORE_AUTH_TOKEN, "bar"),
      connectionParams.getExtraInfos());

    // Test that we don't decode URL more than once
    connectionParams = driver.parseConnectionUrl(baseUrl + "?" +
                                                 ExploreDriver.ConnectionParams.Info.EXPLORE_AUTH_TOKEN.getName() +
                                                 "=AgxqdWxpZW4AyIOOuPRRyJPy%2BPhR4o%2B35wl%3DAA3");
    Assert.assertEquals(
      ImmutableMultimap.of(ExploreDriver.ConnectionParams.Info.EXPLORE_AUTH_TOKEN,
                           "AgxqdWxpZW4AyIOOuPRRyJPy+PhR4o+35wl=AA3"),
      connectionParams.getExtraInfos());
 }

  @Test
  public void testDriverConnection() throws Exception {
    ExploreDriver driver = new ExploreDriver();

    // Wrong URL format
    Assert.assertNull(driver.connect("foobar", null));

    // Correct format but wrong host
    try {
      driver.connect(Constants.Explore.Jdbc.URL_PREFIX + "foo:10000", null);
      Assert.fail();
    } catch (SQLException e) {
      // Expected, host is not available (random host)
    }

    // Correct host
    Assert.assertNotNull(driver.connect(exploreServiceUrl, null));

    // Correct host and extra parameter
    Assert.assertNotNull(driver.connect(exploreServiceUrl + "&auth.token=bar", null));

    // Correct host and extra parameter
    Assert.assertNotNull(driver.connect(exploreServiceUrl + "&auth.token", null));
  }

  @Test
  public void testExploreDriver() throws Exception {
    Connection connection = DriverManager.getConnection(exploreServiceUrl);
    PreparedStatement statement;
    ResultSet resultSet;

    // Use statement.executeQuery
    statement = connection.prepareStatement("fake sql query");
    resultSet = statement.executeQuery();

    Assert.assertEquals(resultSet, statement.getResultSet());
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(1, resultSet.getInt(1));
    Assert.assertEquals("one", resultSet.getString(2));
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(2, resultSet.getInt(1));
    Assert.assertEquals("two", resultSet.getString(2));
    Assert.assertFalse(resultSet.next());

    resultSet.close();
    try {
      resultSet.next();
    } catch (SQLException e) {
      // Expected exception: resultSet is closed
    }
    statement.close();

    // Use statement.execute
    statement = connection.prepareStatement("fake sql query 2");
    Assert.assertTrue(statement.execute());
    resultSet = statement.getResultSet();

    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(1, resultSet.getInt("column1"));
    Assert.assertEquals("one", resultSet.getString("column2"));
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals(2, resultSet.getInt("column1"));
    Assert.assertEquals("two", resultSet.getString("column2"));
    Assert.assertFalse(resultSet.next());

    resultSet.close();
    try {
      resultSet.next();
    } catch (SQLException e) {
      // Expected exception: resultSet is closed
    }
    statement.close();
  }

  @Test(timeout = 2000L)
  public void testCancelQuery() throws Exception {
    Connection connection = DriverManager.getConnection(exploreServiceUrl);
    final PreparedStatement statement = connection.prepareStatement(MockExploreExecutorHandler.LONG_RUNNING_QUERY);

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Wait to make sure that statement.execute() is running
          // NOTE: There can still be a race condition if the execute method fails to set the handle early enough
          TimeUnit.MILLISECONDS.sleep(200);
          statement.cancel();
        } catch (Exception e) {
          Throwables.propagate(e);
        }
      }
    }).start();

    Assert.assertFalse(statement.execute());
    ResultSet rs = statement.getResultSet();
    Assert.assertNull(rs);
  }

  @Path(Constants.Gateway.API_VERSION_3)
  public static class MockExploreExecutorHandler extends AbstractHttpHandler {
    static final String LONG_RUNNING_QUERY = "long_running_query";

    private static final Set<String> handleWithFetchedResutls = Sets.newHashSet();
    private static final Set<String> closedHandles = Sets.newHashSet();
    private static final Set<String> canceledHandles = Sets.newHashSet();
    private static final Set<String> longRunningQueries = Sets.newHashSet();

    @GET
    @Path("explore/status")
    public void status(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "OK.\n");
    }

    @POST
    @Path("namespaces/{namespace-id}/data/explore/queries")
    public void query(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) {
      try {
        QueryHandle handle = QueryHandle.generate();
        Map<String, String> args = decodeArguments(request);
        if (LONG_RUNNING_QUERY.equals(args.get("query"))) {
          longRunningQueries.add(handle.getHandle());
        }
        responder.sendJson(HttpResponseStatus.OK, handle);
      } catch (IOException e) {
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    }

    @DELETE
    @Path("data/explore/queries/{id}")
    public void closeQuery(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
      if (closedHandles.contains(id)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      closedHandles.add(id);
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @GET
    @Path("data/explore/queries/{id}/status")
    public void getQueryStatus(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
      QueryStatus status;
      if (closedHandles.contains(id)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      } else if (canceledHandles.contains(id)) {
        status = new QueryStatus(QueryStatus.OpStatus.CANCELED, false);
      } else if (longRunningQueries.contains(id)) {
        status = new QueryStatus(QueryStatus.OpStatus.RUNNING, false);
      } else {
        status = new QueryStatus(QueryStatus.OpStatus.FINISHED, true);
      }
      responder.sendJson(HttpResponseStatus.OK, status);
    }

    @GET
    @Path("data/explore/queries/{id}/schema")
    public void getQueryResultsSchema(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
      if (closedHandles.contains(id)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      List<ColumnDesc> schema = ImmutableList.of(
          new ColumnDesc("column1", "INT", 1, ""),
          new ColumnDesc("column2", "STRING", 2, "")
      );
      responder.sendJson(HttpResponseStatus.OK, schema);
    }

    @POST
    @Path("data/explore/queries/{id}/next")
    public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
      if (closedHandles.contains(id)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      List<QueryResult> rows = Lists.newArrayList();
      if (!canceledHandles.contains(id) && !handleWithFetchedResutls.contains(id)) {
        rows.add(new QueryResult(ImmutableList.<Object>of("1", "one")));
        rows.add(new QueryResult(ImmutableList.<Object>of("2", "two")));
        handleWithFetchedResutls.add(id);
      }
      responder.sendJson(HttpResponseStatus.OK, rows);
    }

    private Map<String, String> decodeArguments(HttpRequest request) throws IOException {
      ChannelBuffer content = request.getContent();
      if (!content.readable()) {
        return ImmutableMap.of();
      }
      Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
      try {
        Map<String, String> args = new Gson().fromJson(reader, new TypeToken<Map<String, String>>() { }.getType());
        return args == null ? ImmutableMap.<String, String>of() : args;
      } catch (JsonSyntaxException e) {
        throw e;
      } finally {
        reader.close();
      }
    }
  }
}
