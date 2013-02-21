package com.continuuity.gateway.connector;

import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentServerFactory;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.services.InMemoryAppFabricServerFactory;
import com.continuuity.internal.app.store.MDSStoreFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
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
  private final static String CONTINUUITY_API_KEY = Constants.CONTINUUITY_API_KEY_HEADER;
  private final static String CONTINUUITY_JAR_FILE_NAME = "X-Continuuity-JarFileName";

  private static class SimpleDeploymentServerModule extends AbstractModule {
    /**
     * Configures a {@link com.google.inject.Binder} via the exposed methods.
     */
    @Override
    protected void configure() {
      bind(DeploymentServerFactory.class).to(InMemoryAppFabricServerFactory.class);
      bind(LocationFactory.class).to(LocalLocationFactory.class);
      bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
      bind(ManagerFactory.class).to(SyncManagerFactory.class);
      bind(StoreFactory.class).to(MDSStoreFactory.class);
      bind(MetaDataStore.class).to(SerializingMetaDataStore.class);
      bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);
      bind(MetadataService.Iface.class).to(com.continuuity.metadata.MetadataService.class);
    }
  }

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

  CConfiguration configuration;

  AppFabricService.Iface server;

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

    final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                                   new SimpleDeploymentServerModule());
    DeploymentServerFactory factory = injector.getInstance(DeploymentServerFactory.class);

    configuration = CConfiguration.create();
    configuration.set("app.output.dir", "/tmp/app");
    configuration.set("app.tmp.dir", "/tmp/temp");

    // Create the server.
    server = factory.create(configuration);
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
          byte[] jarFileBytes = new byte[length];
          content.readBytes(jarFileBytes);
          String apiKey=request.getHeader(CONTINUUITY_API_KEY);
          try {
            String accountId=getAccountId(configuration.get("passport.hostname", "localhost"),
                                          request.getHeader(CONTINUUITY_API_KEY));
            String jarFileName=request.getHeader(CONTINUUITY_JAR_FILE_NAME);
            deploy(server, configuration, jarFileName, jarFileBytes, accountId, apiKey);
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

  public void deploy(AppFabricService.Iface server,
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
    ri.setFilename(jarFileName);
    ri.setSize(jarFileBytes.length);
    ResourceIdentifier id = server.init(token, ri);
    // Upload the jar file to remote location.
    byte[] toSubmit=jarFileBytes;
    server.chunk(token, id, ByteBuffer.wrap(toSubmit));
    DeploymentStatus status = server.dstatus(token, id);


    server.deploy(token, id);
    int dstatus = server.dstatus(token, id).getOverall();
    while(dstatus == 3) {
      dstatus = server.dstatus(token, id).getOverall();
      Thread.sleep(100);
    }

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
  private String getEndPoint(String hostname, String endpoint){
    return String.format("http://%s:7777/%s",hostname,endpoint);
  }
}
