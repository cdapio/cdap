package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

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
  final AsyncHttpClient asyncHttpClient;

  @Inject
  public ProcedureHandler(GatewayAuthenticator authenticator, DiscoveryServiceClient discoveryServiceClient) {
    super(authenticator);
    this.discoveryServiceClient = discoveryServiceClient;

    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();
    this.asyncHttpClient = new AsyncHttpClient(new NettyAsyncHttpProvider(configBuilder.build()),
                                               configBuilder.build());
  }

  @Override
  public void destroy(HandlerContext context) {
    asyncHttpClient.close();
  }

  @POST
  @Path("/apps/{appId}/procedures/{procedureName}/methods/{methodName}")
  public void procedureCall(HttpRequest request, final HttpResponder responder,
                            @PathParam("appId") String appId, @PathParam("procedureName") String procedureName,
                            @PathParam("methodName") String methodName) {

    Request postRequest;
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
      String relayUri = Joiner.on('/').appendTo(
        new StringBuilder("http://").append(endpoint.getHostName()).append(":").append(endpoint.getPort()).append("/"),
        "apps", appId, "procedures", procedureName, methodName).toString();

      LOG.trace("Relaying request to " + relayUri);

      RequestBuilder requestBuilder = new RequestBuilder("POST");
      postRequest = requestBuilder
        .setUrl(relayUri)
        .setBody(request.getContent().array())
        .build();


      final Request relayRequest = postRequest;
      asyncHttpClient.executeRequest(postRequest,
                                     new AsyncCompletionHandler<Void>() {
                                       @Override
                                       public Void onCompleted(Response response) throws Exception {
                                         if (response.getStatusCode() == OK.getCode()) {
                                           String contentType = response.getContentType();
                                           ChannelBuffer content;
                                           int contentLength = Integer.parseInt(response.getHeader(CONTENT_LENGTH));
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
                                           responder.sendContent(OK,
                                                                 ChannelBuffers.wrappedBuffer(
                                                                   response.getResponseBodyAsByteBuffer()),
                                                                 contentType,
                                                                 ImmutableListMultimap.<String, String>of());
                                         } else {
                                           responder.sendStatus(HttpResponseStatus.valueOf(response.getStatusCode()));
                                         }
                                         return null;
                                       }

                                       @Override
                                       public void onThrowable(Throwable t) {
                                         LOG.trace("Got exception while posting {}", relayRequest, t);
                                         responder.sendStatus(INTERNAL_SERVER_ERROR);
                                       }
                                     });
    } catch (SecurityException e) {
      responder.sendStatus(FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(BAD_REQUEST);
    }  catch (Throwable e) {
      responder.sendStatus(INTERNAL_SERVER_ERROR);
    }
  }
}
