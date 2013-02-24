package com.continuuity.gateway.connector;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.service.ServerException;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.passport.http.client.PassportClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
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
  private final static String CONTINUUITY_JAR_FILE_NAME = "X-Continuuity-JarFileName";


  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {
      HttpMethod.GET,
      HttpMethod.PUT,
      HttpMethod.POST
  };

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private AppFabricRestConnector connector;

  /**
   * The metrics object of the rest connector
   */
  private CMetrics metrics;

  private String pathPrefix;

  CConfiguration configuration=CConfiguration.create();

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
                              MessageEvent message)
    throws Exception {
    HttpRequest request = (HttpRequest) message.getMessage();
    CConfiguration configuration = this.connector.getConfiguration();

    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();

    LOG.trace("Request received: " + method + " " + requestUri);
    MetricsHelper helper = new MetricsHelper(this.getClass(), this.metrics, this.connector.getMetricsQualifier());

    try {
      // check whether the request's HTTP method is supported
      if (method != HttpMethod.GET && method != HttpMethod.PUT && method != HttpMethod.POST) {
        LOG.trace("Received a " + method + " request, which is not supported");
        respondNotAllowed(message.getChannel(), allowedMethods);
        helper.finish(BadRequest);
        return;
      }

      // based on the request URL, determine what to do
      QueryStringDecoder decoder = new QueryStringDecoder(requestUri);
      Map<String, List<String>> parameters = decoder.getParameters();
      List<String> clearParams = null;
      int operation = UNKNOWN;
      
      // if authentication is enabled, verify an authentication token has been
      // passed and then verify the token is valid
      if (!connector.getAuthenticator().authenticateRequest(request)) {
        LOG.info("Received an unauthorized request");
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        helper.finish(BadRequest);
        return;
      }

      String accountId = connector.getAuthenticator().getAccountId(request);

      if (method == HttpMethod.PUT || method == HttpMethod.POST) {
        if (requestUri.endsWith("/deploy")) {
          operation = DEPLOY;
          helper.setMethod("deploy");
        } else {
          operation = BAD;
        }
      } else if (method == HttpMethod.GET) {
        if ("/ping".equals(requestUri)) {
          operation = PING;
          helper.setMethod("ping");
        } else {
          operation = BAD;
        }
      }

      // respond with error for bad requests
      if (operation == BAD) {
        helper.finish(BadRequest);
        LOG.trace("Received an incomplete request '" + request.getUri() + "'.");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }
      // respond with error for unknown requests
      if (operation == UNKNOWN) {
        helper.finish(BadRequest);
        LOG.trace("Received an unsupported " + method + " request '"
            + request.getUri() + "'.");
        respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
        return;
      }

      // is this a ping? (http://gw:port/ping) if so respond OK and done
      if (PING == operation) {
        respondToPing(message.getChannel(), request);
        helper.finish(Success);
        return;
      }

      String destination = null, key = null;
      String path = decoder.getPath();

      // operation DEPLOY must not have a key
      if ((operation == DEPLOY) &&  (key != null && key.length() > 0)) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with invalid path " + path + "(no key may be given)");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }

      switch(operation) {
        case DEPLOY: {
          // read the content of the jar file from the body of the request
          ChannelBuffer content = request.getContent();
          if (content == null) {
            // PUT or POST without content -> 400 Bad Request
            helper.finish(BadRequest);
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
          int length = content.readableBytes();
          String jarFileName=request.getHeader(CONTINUUITY_JAR_FILE_NAME);
          byte[] jarFileBytes = new byte[length];
          content.readBytes(jarFileBytes);
          int port=configuration.getInt(Constants.CFG_APP_FABRIC_SERVER_PORT, Constants.DEFAULT_APP_FABRIC_SERVER_PORT);
          AppFabricService.Client client=getAppFabricClient("localhost", port);
          try {
            String apiKey=request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY);
            deploy(client, configuration, jarFileName, jarFileBytes, accountId, apiKey);
            respondSuccess(message.getChannel(), request);
            helper.finish(Success);
          } catch (Exception e) {
            // something went wrong, internal error
            helper.finish(Error);
            LOG.error("Error during Deploy: " + e.getMessage(), e);
            respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
          }
          break;
        }
        default: {
          // this should not happen because we checked above -> internal error
          helper.finish(Error);
          respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Exception caught for connector '" + this.connector.getName() + "'. ", e.getCause());
      helper.finish(Error);
      if (message.getChannel().isOpen()) {
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        message.getChannel().close();
      }
    }
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
