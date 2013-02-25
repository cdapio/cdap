package com.continuuity.gateway.connector;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.discovery.Discoverable;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.passport.http.client.PassportClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the rest accessor. At this time it
 * only accepts GET requests to retrieve a value for a key from a named table.
 */
public class AppFabricRestHandler extends NettyRestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricRestHandler.class);
  private final static String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {
      HttpMethod.PUT
  };

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private AppFabricRestConnector connector;

  /**
   * The metrics object of the rest connector
   */
  private final CMetrics metrics;

  private final String pathPrefix;

  /**
   * Constructor requires the connector that created this
   *
   * @param connector the connector that created this
   */
  AppFabricRestHandler(AppFabricRestConnector connector) {
    this.connector = connector;
    this.metrics = connector.getMetricsClient();
    this.pathPrefix =
        connector.getHttpConfig().getPathPrefix() +
        connector.getHttpConfig().getPathMiddle();
  }

  private static final int BAD = -1;
  private static final int UNKNOWN = 0;
  private static final int PING = 6;
  private static final int DEPLOY = 7;


  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {
    HttpRequest request = (HttpRequest) message.getMessage();
    CConfiguration configuration = this.connector.getConfiguration();
    MetricsHelper helper = new MetricsHelper(this.getClass(), this.metrics, this.connector.getMetricsQualifier());

    // We first authenticate user.
    if(! connector.getAuthenticator().authenticateRequest(request)) {
      respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
      helper.finish(BadRequest);
      return;
    }

    // Get the account ID
    String accountId = connector.getAuthenticator().getAccountId(request);
    if(accountId == null || accountId.isEmpty()) {
      respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
      helper.finish(BadRequest);
      return;
    }

    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();

    LOG.trace("Request received: " + method + " " + requestUri);

    try {
      // check whether the request's HTTP method is supported
      if(method != HttpMethod.PUT) {
        LOG.trace("Received Unsupported http request method " + method.getName());
        respondNotAllowed(message.getChannel(), allowedMethods);
        helper.finish(BadRequest);
        return;
      }

      List<Discoverable> endpoints
        = Lists.newArrayList(connector.getDiscoveryServiceClient().discover("app.fabric.service"));
      if(endpoints.isEmpty()) {
        LOG.trace("Received a request for deploy, but AppFabric service was not available.");
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        helper.finish(BadRequest);
        return;
      }
      Collections.shuffle(endpoints);

      InetSocketAddress endpoint = endpoints.get(0).getSocketAddress();
      TTransport transport = new TFramedTransport(new TSocket(endpoint.getHostName(), endpoint.getPort()));
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));

      try {
        if(requestUri.contains("/apps/status")) {
          ResourceIdentifier rIdentifier = new ResourceIdentifier(accountId, "no-app", "no-res", 1);
          DeploymentStatus status = client.dstatus(token, rIdentifier);
          Map<String, String> headers = Maps.newHashMap();
          headers.put(HttpHeaders.Names.CONTENT_TYPE, "application/json");
          respond(message.getChannel(), request, HttpResponseStatus.OK, headers, getJsonStatus(status.getOverall(),
                                                                                               status.getMessage())
            .toString().getBytes(Charset.forName("UTF-8")));
          helper.finish(Success);
        } else {
          // Extract the file name from the header.
          String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);
          if(archiveName == null || archiveName.isEmpty()) {
            LOG.trace("Archive name was not available in the header");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            helper.finish(BadRequest);
            return;
          }

          ChannelBuffer content = request.getContent();
          if(content == null) {
            LOG.trace("No body passed from client");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            helper.finish(BadRequest);
            return;
          }

          ResourceInfo rInfo = new ResourceInfo(accountId, "GWApp", archiveName,1, System.currentTimeMillis()/1000);
          ResourceIdentifier rIdentifier = client.init(token, rInfo);

          while(content.readableBytes() > 0) {
            int bytesToRead = Math.min(1024 * 100, content.readableBytes());
            client.chunk(token, rIdentifier, content.readSlice(bytesToRead).toByteBuffer());
          }

          client.deploy(token, rIdentifier);
          respondSuccess(message.getChannel(), request);
        }
      } finally {
        if(transport.isOpen()) {
          transport.close();
        }
      }
    } catch (Exception e) {
      LOG.debug(StackTraceUtil.toStringStackTrace(e));
      LOG.error("Exception caught for connector '" + this.connector.getName() + "'. ", e.getCause());
      helper.finish(Error);
      if (message.getChannel().isOpen()) {
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        message.getChannel().close();
      }
    }
  }

  private JsonObject getJsonStatus(int status, String message) {
    JsonObject object = new JsonObject();
    object.addProperty("status", status);
    object.addProperty("message", message);
    return object;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx,
                              ExceptionEvent e)
      throws Exception {
    MetricsHelper.meterError(metrics, this.connector.getMetricsQualifier());
    LOG.error("Exception caught for connector '" +
        this.connector.getName() + "'. ", e.getCause());
    if (e.getChannel().isOpen()) {
      respondError(ctx.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }

  public void deploy(AppFabricService.Client client,
                     CConfiguration config,
                     String jarFileName,
                     byte[] jarFileBytes,
                     String accountId,
                     String apiKey)
    throws Exception {

    // Call init to get a session identifier - yes, the name needs to be changed.
    AuthToken token = new AuthToken(apiKey);
    ResourceInfo ri = new ResourceInfo();
    ri.setAccountId(accountId);
    ri.setApplicationId("");
    ri.setFilename(jarFileName);
    ri.setSize(jarFileBytes.length);
    ri.setModtime(System.currentTimeMillis());
    ResourceIdentifier id = client.init(token, ri);
    // Upload the jar file to remote location.
    byte[] toSubmit=jarFileBytes;
    client.chunk(token, id, ByteBuffer.wrap(toSubmit));
    DeploymentStatus status = client.dstatus(token, id);


    client.deploy(token, id);
    int dstatus = client.dstatus(token, id).getOverall();
    while(dstatus == 3) {
      dstatus = client.dstatus(token, id).getOverall();
      Thread.sleep(100);
    }
  }

  private int getAccountId(String hostname, int port, String apiKey) {
    PassportClient ppc=new PassportClient();
    return (ppc.getAccount(hostname, port, apiKey)).get().getAccountId();
  }

  private String httpGet(String url,String apiKey) throws Exception {
    String payload  = null;
    HttpGet get = new HttpGet(url);
    get.addHeader("X",apiKey);
    get.addHeader("X-Continuuity-Signature","abcdef");
    boolean debugEnabled=false;
    if (debugEnabled) {
      System.out.println(String.format ("Headers: %s ",get.getAllHeaders().toString()));
      System.out.println(String.format ("URL: %s ",url));
      System.out.println(String.format("Method: %s ", "GET"));
    }

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      response = client.execute(get);
      payload = IOUtils.toString(response.getEntity().getContent());
      client.getConnectionManager().shutdown();
      if (debugEnabled) {
        System.out.println(String.format("Response status: %d ", response.getStatusLine().getStatusCode()));
      }
    } catch (IOException e) {
      if (debugEnabled)  {
        System.out.println(String.format("Caught exception while running http post call: Exception - %s",e.getMessage()));
        e.printStackTrace();
      }
      throw new RuntimeException(e);
    }
    return payload;
  }
  private String getEndPoint(String hostname, String endpoint){
    return String.format("http://%s:7777/%s",hostname,endpoint);
  }

  private AppFabricService.Client getAppFabricClient(String host, int port)
    throws ServerException {

    TTransport transport = new TFramedTransport(
      new TSocket("localhost", port));
    try {
      transport.open();
    } catch (TTransportException e) {
      e.printStackTrace();
      String message = String.format("Unable to connect to thrift " +
                                       "at %s:%d. Reason: %s", "localhost",
                                     port, e.getMessage());
      LOG.error(message);
      throw new ServerException(message, e);
    }
    // now try to connect the thrift client
    return new AppFabricService.Client(new TBinaryProtocol(transport));
  }
}
