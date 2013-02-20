package com.continuuity.gateway.accessor;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.flow.manager.stubs.DelegationToken;
import com.continuuity.flow.manager.stubs.EntityType;
import com.continuuity.flow.manager.stubs.FARService;
import com.continuuity.flow.manager.stubs.FARServiceException;
import com.continuuity.flow.manager.stubs.FARStatus;
import com.continuuity.flow.manager.stubs.ResourceIdentifier;
import com.continuuity.flow.manager.stubs.ResourceInfo;
import com.continuuity.gateway.util.NettyRestHandler;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.TException;
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
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the rest accessor. At this time it
 * only accepts GET requests to retrieve a value for a key from a named table.
 */
public class AppFabricRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricRestHandler.class);
  private final static String CONTINUUITY_API_KEY = "X-Continuuity-ApiKey";
  public final static String JAR_FILE_NAME = "X-Continuuity-JarFileName";
  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {
      HttpMethod.GET,
      HttpMethod.DELETE,
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

  /**
   * All the paths have to be of the form
   * http://host:port&lt;pathPrefix>&lt;table>/&lt;key>
   * For instance, if config(prefix="/v0.1/" path="table/"),
   * then pathPrefix will be "/v0.1/table/", and a valid request is
   * GET http://host:port/v0.1/table/mytable/12345678
   */
  private String pathPrefix;

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
      if (method != HttpMethod.GET && method != HttpMethod.DELETE &&
          method != HttpMethod.PUT && method != HttpMethod.POST) {
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
      if (method == HttpMethod.PUT) {
        operation = DEPLOY;
        helper.setMethod("deploy");
      }
      else if (method == HttpMethod.POST) {
        clearParams = parameters.get("deploy");
        if (clearParams != null && clearParams.size() > 0) {
          operation = DEPLOY;
          helper.setMethod("deploy");
        } else
          operation = BAD;
      } else if (method == HttpMethod.GET) {
        if ("/ping".equals(requestUri)) {
          operation = PING;
          helper.setMethod("ping");
        }
        else if (parameters == null || parameters.size() == 0) {
          operation = DEPLOY;
          helper.setMethod("deploy");
        } else {
          List<String> qParams = parameters.get("q");
          if (qParams != null && qParams.size() == 1
              && "deploy".equals(qParams.get(0))) {
            operation = DEPLOY;
            helper.setMethod("deploy");
          } else
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

      // respond with error for parameters if the operation does not allow them
      if (operation != DEPLOY  && parameters != null && !parameters.isEmpty()) {
        helper.finish(BadRequest);
        LOG.trace("Received a " + method + " request with query parameters, which is not supported");
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

      // we only support requests of the form
      // http://host:port/prefix/path/<tablename>/<key>
      if (path.startsWith(this.pathPrefix)) {
        String remainder = path.substring(this.pathPrefix.length());
        int pos = remainder.indexOf("/");
        if (pos < 0) {
          destination = remainder.length() == 0 ? null : remainder;
          key = null;
        } else {
          destination = remainder.substring(0, pos);
          // no further / is allowed in the path
          if (remainder.length() == pos + 1) {
            key = null;
          } else if (remainder.indexOf('/', pos + 1) < 0)
            key = remainder.substring(pos + 1);
          else {
            helper.finish(BadRequest);
            LOG.trace("Received a request with invalid path " +
                path + "(path does not end with key)");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          } } }

      // check that URL could be parsed up to destination
      // except for CLEAR, where no destination may be given
      if ((destination == null && operation != DEPLOY) || (destination != null && operation == DEPLOY)) {
        helper.finish(NotFound);
        LOG.trace("Received a request with unknown path '" + path + "'.");
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }

      // all operations except for DEPLOY need a key
      if (operation != DEPLOY  && (key == null || key.length() == 0)) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with invalid path " +
            path + "(no key given)");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }
      // operation LIST and CLEAR must not have a key
      if ((operation == DEPLOY) &&  (key != null && key.length() > 0)) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with invalid path " + path + "(no key may be given)");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }

      String table = "default".equals(destination) ? null : destination;

      // key is URL-encoded, decode it
      byte[] keyBinary = null;
      if (key != null) {
        key = URLDecoder.decode(key, "ISO8859_1");
        LOG.trace("Received " + method + " request for key '" + key + "'.");
        keyBinary = key.getBytes("ISO8859_1");
      }

      switch(operation) {
        case DEPLOY: {
          // read the content of the jar file from the body of the request
          ChannelBuffer content = request.getContent();
          if (content == null) {
            // PUT without content -> 400 Bad Request
            helper.finish(BadRequest);
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
          int length = content.readableBytes();
          byte[] jarFileBytes = new byte[length];
          content.readBytes(jarFileBytes);
          try {
            FARService.Client rClient=null;
            String accountId=getAccountId(configuration.get("passport.hostname", "localhost"),
                                          request.getHeader(CONTINUUITY_API_KEY));
            String jarFileName=request.getHeader(JAR_FILE_NAME);
            ImmutablePair<Boolean, String> ok = deploy(rClient, configuration, jarFileName, jarFileBytes, accountId, true);
            respondSuccess(message.getChannel(), request);
            helper.finish(Success);
          } catch (Exception e) {
            // something went wrong, internal error
            helper.finish(Error);
            LOG.error("Error during Write: " + e.getMessage(), e);
            respondError(message.getChannel(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
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

  /**
   * Connects to FAR service.
   */
  private FARService.Client connectToFARService(FARService.Client rClient,
                                                CConfiguration configuration,
                                                boolean remote)
    throws IOException {
    // If client is not connected or if it was connected,
    // but is no more connected now.
    if (rClient == null || !rClient.getInputProtocol().getTransport().isOpen()) {

      String serverAddress = null;
      int serverPort = -1;

      if(! remote) {
        serverAddress = configuration.get(
          Constants.CFG_RESOURCE_MANAGER_SERVER_ADDRESS,
          Constants.DEFAULT_RESOURCE_MANAGER_SERVER_ADDRESS
        );

        serverPort =
          configuration.getInt(
            Constants.CFG_RESOURCE_MANAGER_SERVER_PORT,
            Constants.DEFAULT_RESOURCE_MANAGER_SERVER_PORT
          );
      } else {
        serverAddress = configuration.get(
          Constants.CFG_RESOURCE_MANAGER_CLOUD_HOST,
          Constants.DEFAULT_RESOURCE_MANAGER_CLOUD_HOST
        );

        serverPort =
          configuration.getInt(
            Constants.CFG_RESOURCE_MANAGER_CLOUD_PORT,
            Constants.DEFAULT_RESOURCE_MANAGER_CLOUD_PORT
          );
      }

      TTransport transport = new TFramedTransport(
        new TSocket(serverAddress, serverPort));

      try {
        transport.open();
      } catch (TTransportException e) {
        throw new IOException("Unable to connect to service", e);
      }

      TProtocol protocol = new TBinaryProtocol(transport);
      rClient = new FARService.Client(protocol);
    }
    return rClient;
  }
  /**
   * Deploys a given application to a remote server.
   *
   * @param
   * @return pair consisting of status and message description.
   * @throws java.io.IOException
   */
  public ImmutablePair<Boolean, String> deploy(FARService.Client rClient,
                                               CConfiguration config,
                                               String jarFileName,
                                               byte[] jarFileBytes,
                                               String accountId,
                                               boolean remoteFARService)
    throws IOException {

    // Check if the local resource exists
    if(jarFileBytes==null || jarFileBytes.length==0) {
      return new ImmutablePair<Boolean, String>(false, String.format("%s does not exists.", jarFileName));
    }

    connectToFARService(rClient, config, remoteFARService);

    // Construct resource info
    ResourceInfo info = new ResourceInfo();
    info.setAccountId(accountId);
    info.setApplicationId(accountId); // can be empty !
    info.setType(EntityType.FLOW); // ignored
    info.setFilename(jarFileName);
//    info.setModtime(file.lastModified());  ????
//    info.setSize((int) file.getTotalSpace());  ????
    byte[] toSubmit=jarFileBytes;
    try {
      // Initialize the resource deployment
      ResourceIdentifier identifier = rClient.init(getDelegationToken(), info);
      if(identifier == null) {
        return new ImmutablePair<Boolean, String>(false, "Unable to initialize deployment phase.");
      }

      rClient.chunk(getDelegationToken(), identifier, ByteBuffer.wrap(toSubmit));

      // Finalize the resource deployment
      rClient.deploy(getDelegationToken(), identifier);


      // If a flow within the resource is running, then we don't
      // successfully deploy the resource
      FARStatus status = rClient.status(getDelegationToken(), identifier);
      if(status.getOverall()
        == com.continuuity.flow.common.ResourceStatus.ALREADY_RUNNING) {
        return new ImmutablePair<Boolean, String>(false, "One of the flow " +
          "from the resource being deployed is already running. " +
          "Stop it first.");
      }

      int count = 0;
      while(status.getOverall()
        == com.continuuity.flow.common.ResourceStatus.VERIFYING) {
        if(count > 5) {
          return new ImmutablePair<Boolean, String>(false,
                                                    "Verification took more than 5 seconds. Timing out. " +
                                                      "Please check contents of the resource file to make sure " +
                                                      "you have not included libs that are not packaged.");
        }
        Thread.sleep(1000);
        count++;
        status = rClient.status(getDelegationToken(), identifier);
      }
      if(status.getOverall()
        == com.continuuity.flow.common.ResourceStatus.VERIFICATION_FAILURE) {
        return new ImmutablePair<Boolean, String>(false, status.getMessage());
      }
    } catch (FARServiceException e) {
      return new ImmutablePair<Boolean, String>(false, e.getMessage());
    } catch (TException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return new ImmutablePair<Boolean, String>(true, "OK");
  }
  private String getAccountId(String hostname, String apiKey) {
    String accountId;
    String url  = getEndPoint(hostname,"passport/v1/vpc");
    try {
      String data = httpGet(url,apiKey);
      if (data!= null ) {
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(data);
        JsonArray jsonArray = element.getAsJsonArray();
        //for( )

        for (JsonElement elements : jsonArray) {
          JsonObject authResponse = elements.getAsJsonObject();
          if (authResponse.get("vpc_name") != null ) {
            return authResponse.get("vpc_name").getAsString();
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
  private String httpGet(String url,String apiKey) throws Exception {
    String payload  = null;
    HttpGet get = new HttpGet(url);
    get.addHeader(CONTINUUITY_API_KEY,apiKey);
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

  private DelegationToken getDelegationToken() {
    return new DelegationToken();
  }
  private String getEndPoint(String hostname, String endpoint){
    return String.format("http://%s:7777/%s",hostname,endpoint);
  }
}
