package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
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
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * Handles procedure calls.
 */
@Path("/v2")
public class ProcedureHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureHandler.class);
  private final DiscoveryServiceClient discoveryServiceClient;
  private final AsyncHttpClient asyncHttpClient;
  private static final Type QUERY_PARAMS_TYPE = new TypeToken<Map<String, String>>() {}.getType();
  private final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
    @Override
    protected Gson initialValue() {
      return new Gson();
    }
  };

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
    LOG.info("Stopping async http client...");
    asyncHttpClient.close();
  }

  @POST
  @Path("/apps/{app-id}/procedures/{procedure-name}/methods/{method-name}")
  public void procedurePost(HttpRequest request, final HttpResponder responder,
                            @PathParam("app-id") String appId, @PathParam("procedure-name") String procedureName,
                            @PathParam("method-name") String methodName) {

    try {

      procedureCall(request, responder, appId, procedureName, methodName, request.getContent().array());

    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/procedures/{procedure-name}/methods/{method-name}")
  public void procedureGet(HttpRequest request, final HttpResponder responder,
                            @PathParam("app-id") String appId, @PathParam("procedure-name") String procedureName,
                            @PathParam("method-name") String methodName) {

    try {
      // Parameters in query string allows multiple values for a singe key, however procedures take one value for one
      // key, so will randomly pick one value for a key.
      byte [] body = null;
      Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();

      if (!queryParams.isEmpty()) {
        String json = gson.get().toJson(Maps.transformEntries(queryParams, MULTIMAP_TO_MAP_FUNCTION),
                                           QUERY_PARAMS_TYPE);
        body = Bytes.toBytes(json);
      }

      procedureCall(request, responder, appId, procedureName, methodName, body);

    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  private void procedureCall(HttpRequest request, final HttpResponder responder,
                            String appId, String procedureName, String methodName,
                            byte [] body) {

    try {
      String accountId = getAuthenticatedAccountId(request);

      // determine the service provider for the given path
      String serviceName = String.format("procedure.%s.%s.%s", accountId, appId, procedureName);
      List<Discoverable> endpoints = Lists.newArrayList(discoveryServiceClient.discover(serviceName));
      if (endpoints.isEmpty()) {
        LOG.trace("No endpoint for service {}", serviceName);
        responder.sendStatus(NOT_FOUND);
        return;
      }

      // make HTTP call to provider
      Collections.shuffle(endpoints);
      InetSocketAddress endpoint = endpoints.get(0).getSocketAddress();
      final String relayUri = Joiner.on('/').appendTo(
        new StringBuilder("http://").append(endpoint.getHostName()).append(":").append(endpoint.getPort()).append("/"),
        "apps", appId, "procedures", procedureName, methodName).toString();

      LOG.trace("Relaying request to " + relayUri);

      // Construct request
      RequestBuilder requestBuilder = new RequestBuilder("POST");
      requestBuilder.setUrl(relayUri);

      if (body != null) {
        requestBuilder.setBody(body);
      }

      // Add headers
      for (Map.Entry<String, String> entry : request.getHeaders()) {
        requestBuilder.addHeader(entry.getKey(), entry.getValue());
      }

      Request postRequest = requestBuilder.build();
      asyncHttpClient.executeRequest(postRequest,
                                     new AsyncCompletionHandler<Void>() {
                                       @Override
                                       public Void onCompleted(Response response) throws Exception {
                                         if (response.getStatusCode() == OK.getCode()) {
                                           String contentType = response.getContentType();
                                           ChannelBuffer content;

                                           int contentLength = getContentLength(response);
                                           if (contentLength > 0) {
                                             content = ChannelBuffers.dynamicBuffer(contentLength);
                                           } else {
                                             // the transfer encoding is usually chunked, so no content length is
                                             // provided. Just trying to read anything
                                             content = ChannelBuffers.dynamicBuffer();
                                           }

                                           // Should not close the inputstream as per Response javadoc
                                           InputStream input = response.getResponseBodyAsStream();
                                           ByteStreams.copy(input, new ChannelBufferOutputStream(content));

                                           // Copy headers
                                           ImmutableListMultimap.Builder<String, String> headerBuilder =
                                             ImmutableListMultimap.builder();
                                           for (Map.Entry<String, List<String>> entry : response.getHeaders()) {
                                             headerBuilder.putAll(entry.getKey(), entry.getValue());
                                           }

                                           responder.sendContent(OK,
                                                                 content,
                                                                 contentType,
                                                                 headerBuilder.build());
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
      responder.sendStatus(FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(BAD_REQUEST);
    }  catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }

  private int getContentLength(Response response) {
    try {
      return Integer.parseInt(response.getHeader(CONTENT_LENGTH));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

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
}
