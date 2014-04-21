package com.continuuity.passport.testhelper;

import com.continuuity.common.conf.Constants;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Forwards calls to passport server.
 */
public class TestAccountServer extends AbstractIdleService {
  private final NettyHttpService server;
  private final int passportServerPort;

  public TestAccountServer(int passportServerPort) {
    this.passportServerPort = passportServerPort;
    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(ImmutableList.of(new AccountHandler()));
    server = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    server.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    server.stopAndWait();
  }

  public int getPort() {
    return server.getBindAddress().getPort();
  }

  /**
   *
   */
  public final class AccountHandler extends AbstractHttpHandler {

    @GET
    @Path("/api/whois/{apiKey}")
    public void getAccountId(HttpRequest request, HttpResponder responder, @PathParam("apiKey") String apiKey)
    throws Exception {
      HttpResponse response =
        doPost("/passport/v1/accounts/authenticate",
               new Header[]{new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, apiKey)});
      responder.sendString(HttpResponseStatus.valueOf(response.getStatusLine().getStatusCode()),
                           EntityUtils.toString(response.getEntity()));
    }

    @GET
    @Path("/api/vpc/list/{apiKey}")
    public void getVpcList(HttpRequest request, HttpResponder responder, @PathParam("apiKey") String apiKey)
      throws Exception {
      HttpResponse response =
        doGet("/passport/v1/clusters",
              new Header[]{new BasicHeader(Constants.Gateway.CONTINUUITY_API_KEY, apiKey)});
      responder.sendString(HttpResponseStatus.valueOf(response.getStatusLine().getStatusCode()),
                           EntityUtils.toString(response.getEntity()));
    }

    public HttpResponse doPost(String resource, Header[] headers) throws Exception {
      DefaultHttpClient client = new DefaultHttpClient();
      HttpPost post = new HttpPost("http://localhost" + ":" + passportServerPort + resource);

      if (headers != null) {
        post.setHeaders(headers);
      }
      return client.execute(post);
    }

    public HttpResponse doGet(String resource, Header[] headers) throws Exception {
      DefaultHttpClient client = new DefaultHttpClient();
      HttpGet get = new HttpGet("http://localhost" + ":" + passportServerPort + resource);

      if (headers != null) {
        get.setHeaders(headers);
      }
      return client.execute(get);
    }
  }
}
