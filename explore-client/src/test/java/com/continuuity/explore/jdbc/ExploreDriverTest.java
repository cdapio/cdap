package com.continuuity.explore.jdbc;

import com.continuuity.common.conf.Constants;
import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.Result;
import com.continuuity.explore.service.Status;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

    Class.forName("com.continuuity.explore.jdbc.ExploreDriver");
    exploreServiceUrl = String.format("%s%s:%d", Constants.Explore.Jdbc.URL_PREFIX, "localhost", httpService.getPort());
  }

  @AfterClass
  public static void stop() throws Exception {
    httpService.stopAndWait();
  }

  @Test
  public void testDriverConnection() throws Exception {
    ExploreDriver driver = new ExploreDriver();

    // Wrong URL format
    Assert.assertNull(driver.connect("foobar", null));

    // Correct format but wrong host
    try {
      driver.connect(Constants.Explore.Jdbc.URL_PREFIX + "foo:10000", null);
    } catch (SQLException e) {
      // Expected, host is not available (random host)
    }

    // Correct host
    Assert.assertNotNull(driver.connect(exploreServiceUrl, null));
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

  public static class MockExploreExecutorHandler extends AbstractHttpHandler {
    static final String LONG_RUNNING_QUERY = "long_running_query";

    private static Set<String> handleWithFetchedResutls = Sets.newHashSet();
    private static Set<String> closedHandles = Sets.newHashSet();
    private static Set<String> canceledHandles = Sets.newHashSet();
    private static Set<String> longRunningQueries = Sets.newHashSet();

    @GET
    @Path("v2/explore/status")
    public void status(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "OK.\n");
    }

    @POST
    @Path("v2/data/queries")
    public void query(HttpRequest request, HttpResponder responder) {
      try {
        Handle handle = Handle.generate();
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
    @Path("v2/data/queries/{id}")
    public void closeQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                           @PathParam("id") final String id) {
      if (closedHandles.contains(id)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      closedHandles.add(id);
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @POST
    @Path("v2/data/queries/{id}/cancel")
    public void cancelQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                            @PathParam("id") final String id) {
      if (closedHandles.contains(id)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      canceledHandles.add(id);
      responder.sendStatus(HttpResponseStatus.OK);
    }

    @GET
    @Path("v2/data/queries/{id}/status")
    public void getQueryStatus(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                               @PathParam("id") final String id) {
      Status status = null;
      if (closedHandles.contains(id)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      } else if (canceledHandles.contains(id)) {
        status = new Status(Status.OpStatus.CANCELED, false);
      } else if (longRunningQueries.contains(id)) {
        status = new Status(Status.OpStatus.RUNNING, false);
      } else {
        status = new Status(Status.OpStatus.FINISHED, true);
      }
      responder.sendJson(HttpResponseStatus.OK, status);
    }

    @GET
    @Path("v2/data/queries/{id}/schema")
    public void getQueryResultsSchema(@SuppressWarnings("UnusedParameters") HttpRequest request,
                                      HttpResponder responder, @PathParam("id") final String id) {
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
    @Path("v2/data/queries/{id}/next")
    public void getQueryNextResults(@SuppressWarnings("UnusedParameters") HttpRequest request,
                                    HttpResponder responder, @PathParam("id") final String id) {
      if (closedHandles.contains(id)) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }
      List<Result> rows = Lists.newArrayList();
      if (!canceledHandles.contains(id) && !handleWithFetchedResutls.contains(id)) {
        rows.add(new Result(ImmutableList.<Object>of("1", "one")));
        rows.add(new Result(ImmutableList.<Object>of("2", "two")));
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
