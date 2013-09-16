/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.collect.ImmutableListMultimap;
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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;


/**
 *
 */
public final class WorkflowHandler extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowHandler.class);
  private final DiscoveryServiceClient discoveryServiceClient;
  private final AsyncHttpClient asyncHttpClient;

  @Inject
  public WorkflowHandler(GatewayAuthenticator authenticator, DiscoveryServiceClient discoveryServiceClient) {
    super(authenticator);
    this.discoveryServiceClient = discoveryServiceClient;

    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();
    this.asyncHttpClient = new AsyncHttpClient(new NettyAsyncHttpProvider(configBuilder.build()),
                                               configBuilder.build());
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting WorkflowHandler.");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping WorkflowHandler.");
    asyncHttpClient.close();
  }

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-name}/status")
  public void workflowStatus(HttpRequest request, final HttpResponder responder,
                             @PathParam("app-id") String appId, @PathParam("workflow-name") String workflowName) {

    try {
      fetchStatus(request, responder, appId, workflowName);
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }


  private void fetchStatus(HttpRequest request, final HttpResponder responder,
                           String appId, String workflowName) {
    try {
      String accountId = getAuthenticatedAccountId(request);

      // determine the service provider for the given path
      String serviceName = String.format("workflow.%s.%s.%s", accountId, appId, workflowName);
      Discoverable discoverable = new RandomEndpointStrategy(discoveryServiceClient.discover(serviceName)).pick();

      if (discoverable == null) {
        LOG.info("No endpoint for service {}", serviceName);
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        return;
      }

      // make HTTP call to workflow service.
      InetSocketAddress endpoint = discoverable.getSocketAddress();
      // Construct request
      String url = String.format("http://%s:%d/status", endpoint.getHostName(), endpoint.getPort());
      Request workflowRequest = new RequestBuilder("GET").setUrl(url).build();

      asyncHttpClient.executeRequest(workflowRequest, new AsyncCompletionHandler<Void>() {
        @Override
        public Void onCompleted(Response response) throws Exception {
          if (response.getStatusCode() == HttpResponseStatus.OK.getCode()) {
            // Simply write through
            response.getResponseBodyAsByteBuffer();
          }
        }
      });
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
}
