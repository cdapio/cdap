package com.continuuity.gateway.router.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.security.auth.AccessTokenTransformer;
import com.continuuity.security.auth.TokenValidator;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Security handler that intercept HTTP message and validates the access token in
 * header Authorization field.
 */
public class SecurityAuthenticationHttpHandler extends SimpleChannelUpstreamHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityAuthenticationHttpHandler.class);

  private final TokenValidator tokenValidator;
  private final AccessTokenTransformer accessTokenTransformer;
  DiscoveryServiceClient discoveryServiceClient;
  private final boolean securityEnabled;
  private final String realm;

  public SecurityAuthenticationHttpHandler(String realm, TokenValidator tokenValidator,
                                           AccessTokenTransformer accessTokenTransformer, boolean securityEnabled,
                                           DiscoveryServiceClient discoveryServiceClient) {
    this.realm = realm;
    this.tokenValidator = tokenValidator;
    this.accessTokenTransformer = accessTokenTransformer;
    this.securityEnabled = securityEnabled;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  /**
   * Intercepts the HttpMessage for getting the access token in authorization header
   * and validates it for authentication.
   * @param ctx channel handler context delegated from MessageReceived callback
   * @param event Message event delegated from MessageReceived callback
   * @throws Exception
   */
  private void securedInterception(ChannelHandlerContext ctx, MessageEvent event) throws Exception {
    final HttpRequest msg = (HttpRequest) event.getMessage();
    JsonObject jsonObject = new JsonObject();
    // Suspend incoming traffic until connected to the outbound service.
    final Channel inboundChannel = event.getChannel();
    inboundChannel.setReadable(false);

    String auth = msg.getHeader(HttpHeaders.Names.AUTHORIZATION);
    String accessToken = null;

    if (auth != null) {
      int spIndex = auth.trim().indexOf(' ') + 1;
      if (spIndex != -1) {
        accessToken = auth.substring(spIndex).trim();
      }
    }
    TokenValidator.State tokenState = tokenValidator.validate(accessToken);
    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
    switch (tokenState) {
      case TOKEN_MISSING:
        httpResponse.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE,
                               String.format("Bearer realm=\"%s\"", realm));
        jsonObject.addProperty("error", "Token Missing");
        jsonObject.addProperty("error_description", tokenState.getMsg());
        LOG.info("Failed authentication due to missing token");
        break;

      case TOKEN_INVALID:
      case TOKEN_EXPIRED:
      case TOKEN_INTERNAL:
        httpResponse.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE,
                               String.format("Bearer realm=\"%s\" error=\"invalid_token\"" +
                                               "error_description=\"%s\"", realm, tokenState.getMsg()));
        jsonObject.addProperty("error", "invalid_token");
        jsonObject.addProperty("error_description", tokenState.getMsg());
        LOG.info("Failed authentication due to invalid token, reason={};", tokenState);
        break;
    }
    if (tokenState != TokenValidator.State.TOKEN_VALID) {
      JsonArray externalAuthenticationURIs = new JsonArray();
      stopWatchWait(externalAuthenticationURIs);
      jsonObject.add("auth_uri", externalAuthenticationURIs);

      ChannelBuffer content = ChannelBuffers.wrappedBuffer(jsonObject.toString().getBytes(Charsets.UTF_8));
      httpResponse.setContent(content);
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, content.readableBytes());
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json;charset=UTF-8");

      ChannelFuture writeFuture = Channels.future(event.getChannel());
      Channels.write(ctx, writeFuture, httpResponse);
      writeFuture.addListener(ChannelFutureListener.CLOSE);
      return;
    } else {
      String serealizedAccessTokenIdentifier = accessTokenTransformer.transform(accessToken.trim());
      msg.setHeader(HttpHeaders.Names.WWW_AUTHENTICATE, "Reactor-verified " + serealizedAccessTokenIdentifier);
      inboundChannel.setReadable(true);
      Channels.fireMessageReceived(ctx, msg, event.getRemoteAddress());
    }
  }

  private void stopWatchWait(JsonArray externalAuthenticationURIs) throws Exception{
    boolean done = false;
    Iterable<Discoverable> discoverables = discoveryServiceClient.discover(Constants.Service.EXTERNAL_AUTHENTICATION);
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      for (Discoverable d : discoverables)  {
        externalAuthenticationURIs.add(new JsonPrimitive(d.getSocketAddress().getAddress().getHostAddress()));
        done = true;
      }
      if (!done) {
        TimeUnit.MILLISECONDS.sleep(200);
      }
    } while (!done && stopwatch.elapsedTime(TimeUnit.SECONDS) < 2L);
  }


  @Override
  public void messageReceived(ChannelHandlerContext ctx, final MessageEvent event) throws Exception {
    if (event.getMessage() instanceof HttpChunk) {
      super.messageReceived(ctx, event);
    } else if (securityEnabled) {
      securedInterception(ctx, event);
    } else {
      super.messageReceived(ctx, event);
    }
  }
}
