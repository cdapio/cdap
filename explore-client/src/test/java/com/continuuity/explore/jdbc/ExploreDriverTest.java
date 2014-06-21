package com.continuuity.explore.jdbc;

import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.Result;
import com.continuuity.explore.service.Status;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
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

    // Register explore jdbc driver
    Class.forName("com.continuuity.explore.jdbc.ExploreDriver");
    exploreServiceUrl = String.format("%s%s:%d", ExploreJDBCUtils.URL_PREFIX, "localhost", httpService.getPort());
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
      driver.connect(ExploreJDBCUtils.URL_PREFIX + "foo:10000", null);
    } catch (SQLException e) {
      // Expected, host is not available (random host)
    }

    // Correct host
    Assert.assertNotNull(driver.connect(exploreServiceUrl, null));
  }

  @Test
  public void testExploreDriver() throws Exception {
    Connection connection = DriverManager.getConnection(exploreServiceUrl);
    PreparedStatement statement = connection.prepareStatement("show tables");
    ResultSet resultSet = statement.executeQuery();

    Assert.assertTrue(resultSet.next());
    Assert.assertEquals("1", resultSet.getString(1));
    Assert.assertEquals("one", resultSet.getString(2));
    Assert.assertTrue(resultSet.next());
    Assert.assertEquals("2", resultSet.getString(1));
    Assert.assertEquals("two", resultSet.getString(2));
    Assert.assertFalse(resultSet.next());

    resultSet.close();
    try {
      resultSet.next();
    } catch (SQLException e) {
      // Expected exception: resultSet is closed
    }
    statement.close();
  }

  public static class MockExploreExecutorHandler extends AbstractHttpHandler {

    List<String> handleWithFetchedResutls = Lists.newArrayList();

    @GET
    @Path("v2/explore/status")
    public void status(HttpRequest request, HttpResponder responder) {
      responder.sendString(HttpResponseStatus.OK, "OK.\n");
    }

    @POST
    @Path("v2/data/queries")
    public void query(HttpRequest request, HttpResponder responder) {
      Handle handle = Handle.generate();
      responder.sendJson(HttpResponseStatus.OK, handle);
    }

    @DELETE
    @Path("v2/data/queries/{id}")
    public void closeQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                           @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "stop:" + id);
    }

    @POST
    @Path("v2/data/queries/{id}/cancel")
    public void cancelQuery(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                            @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "cancel:" + id);
    }

    @GET
    @Path("v2/data/queries/{id}/status")
    public void getQueryStatus(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder,
                               @PathParam("id") final String id) {
      Status status = new Status(Status.OpStatus.FINISHED, true);
      responder.sendJson(HttpResponseStatus.OK, status);
    }

    @GET
    @Path("v2/data/queries/{id}/schema")
    public void getQueryResultsSchema(@SuppressWarnings("UnusedParameters") HttpRequest request,
                                      HttpResponder responder, @PathParam("id") final String id) {
      responder.sendString(HttpResponseStatus.OK, "schema:" + id);
    }

    @POST
    @Path("v2/data/queries/{id}/nextResults")
    public void getQueryNextResults(HttpRequest request, HttpResponder responder, @PathParam("id") final String id) {
      List<Result> rows = Lists.newArrayList();
      if (!handleWithFetchedResutls.contains(id)) {
        rows.add(new Result(ImmutableList.<Object>of("1", "one")));
        rows.add(new Result(ImmutableList.<Object>of("2", "two")));
        handleWithFetchedResutls.add(id);
      }
      responder.sendJson(HttpResponseStatus.OK, rows);
    }
  }
}
