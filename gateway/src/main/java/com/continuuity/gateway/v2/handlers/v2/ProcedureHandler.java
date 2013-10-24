package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import com.ning.http.client.providers.netty.NettyResponse;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;

/**
 * Handles procedure calls.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ProcedureHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureHandler.class);
  private static final Type QUERY_PARAMS_TYPE = new TypeToken<Map<String, String>>() {}.getType();
  private static final Gson GSON = new Gson();
  private static final long DISCOVERY_TIMEOUT_SECONDS = 1L;
  private static final Maps.EntryTransformer<String, List<String>, String> MULTIMAP_TO_MAP_FUNCTION =
    new Maps.EntryTransformer<String, List<String>, String>() {
      @Override
      public String transformEntry(@Nullable String key, @Nullable List<String> value) {
        if (value == null || value.isEmpty()) {
          return null;
        }
        return value.get(0);
      }
    };

  private final DiscoveryServiceClient discoveryServiceClient;
  private final AsyncHttpClient asyncHttpClient;

  @Inject
  public ProcedureHandler(GatewayAuthenticator authenticator, DiscoveryServiceClient discoveryServiceClient) {
    super(authenticator);
    this.discoveryServiceClient = discoveryServiceClient;

    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();
    this.asyncHttpClient = new AsyncHttpClient(new NettyAsyncHttpProvider(configBuilder.build()),
                                               configBuilder.build());
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting ProcedureHandler.");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping ProcedureHandler.");
    asyncHttpClient.close();
  }

  @POST
  @Path("/apps/{app-id}/procedures/{procedure-name}/methods/{method-name}")
  public void procedurePost(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") String appId,
                            @PathParam("procedure-name") String procedureName,
                            @PathParam("method-name") String methodName) {

    procedureCall(request, responder, appId, procedureName, methodName, request.getContent());
  }

  @GET
  @Path("/apps/{app-id}/procedures/{procedure-name}/methods/{method-name}")
  public void procedureGet(HttpRequest request, final HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("procedure-name") String procedureName,
                           @PathParam("method-name") String methodName) {

    try {
      // Parameters in query string allows multiple values for a singe key, however procedures take one value for one
      // key, so will randomly pick one value for a key.
      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
      ChannelBuffer content = ChannelBuffers.EMPTY_BUFFER;

      if (!queryParams.isEmpty()) {
        content = jsonEncode(Maps.transformEntries(queryParams, MULTIMAP_TO_MAP_FUNCTION), QUERY_PARAMS_TYPE,
                             ChannelBuffers.dynamicBuffer(request.getUri().length()));
      }

      procedureCall(request, responder, appId, procedureName, methodName, content);

    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  private void procedureCall(HttpRequest request, final HttpResponder responder,
                             String appId, String procedureName, String methodName,
                             ChannelBuffer content) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      // determine the service provider for the given path
      String serviceName = String.format("procedure.%s.%s.%s", accountId, appId, procedureName);
      Discoverable discoverable = new TimeLimitEndpointStrategy(
                                      new RandomEndpointStrategy(discoveryServiceClient.discover(serviceName)),
                                      DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                  .pick();

      if (discoverable == null) {
        LOG.error("No endpoint for service {}", serviceName);
        responder.sendStatus(NOT_FOUND);
        return;
      }

      // make HTTP call to provider
      InetSocketAddress endpoint = discoverable.getSocketAddress();

      final String relayUri = String.format("http://%s:%d/apps/%s/procedures/%s/%s",
                                            endpoint.getHostName(), endpoint.getPort(),
                                            appId, procedureName, methodName);

      LOG.trace("Relaying request to " + relayUri);

      // Construct request
      RequestBuilder requestBuilder = new RequestBuilder("POST");
      requestBuilder.setUrl(relayUri);

      if (content.readable()) {
        requestBuilder.setBody(new ChannelBufferEntityWriter(content), content.readableBytes());
      }

      // Add headers
      for (Map.Entry<String, String> entry : request.getHeaders()) {
        requestBuilder.addHeader(entry.getKey(), entry.getValue());
      }

      asyncHttpClient.executeRequest(requestBuilder.build(), new AsyncCompletionHandler<Void>() {
        @Override
        public Void onCompleted(Response response) throws Exception {
          if (response.getStatusCode() == OK.getCode()) {
            String contentType = response.getContentType();
            ChannelBuffer content = getResponseBody(response);

            // Copy headers
            ImmutableListMultimap.Builder<String, String> headerBuilder = ImmutableListMultimap.builder();
            for (Map.Entry<String, List<String>> entry : response.getHeaders()) {
              headerBuilder.putAll(entry.getKey(), entry.getValue());
            }

            responder.sendContent(OK, content, contentType, headerBuilder.build());
          } else {
            responder.sendStatus(HttpResponseStatus.valueOf(response.getStatusCode()));
          }
          return null;
        }

        @Override
        public void onThrowable(Throwable t) {
          LOG.trace("Got exception while posting {}", relayUri, t);
          responder.sendStatus(INTERNAL_SERVER_ERROR);
        }
      });
    } catch (SecurityException e) {
      responder.sendStatus(UNAUTHORIZED);
    } catch (IllegalArgumentException e) {
      responder.sendString(BAD_REQUEST, e.getMessage());
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  private ChannelBuffer getResponseBody(Response response) throws IOException {
    // Optimization for NettyAsyncHttpProvider
    if (response instanceof NettyResponse) {
      // This avoids copying
      return ((NettyResponse) response).getResponseBodyAsChannelBuffer();
    }
    // This may copy, depending on the Response implementation.
    return ChannelBuffers.wrappedBuffer(response.getResponseBodyAsByteBuffer());
  }

  private <T> ChannelBuffer jsonEncode(T obj, Type type, ChannelBuffer buffer) throws IOException {
    Writer writer = new OutputStreamWriter(new ChannelBufferOutputStream(buffer), Charsets.UTF_8);
    try {
      GSON.toJson(obj, type, writer);
    } finally {
      writer.close();
    }
    return buffer;
  }
}
