/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.router.handlers;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.AuditLogEntry;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.security.auth.AccessTokenTransformer;
import co.cask.cdap.security.auth.TokenState;
import co.cask.cdap.security.auth.TokenValidator;
import co.cask.cdap.security.server.GrantAccessToken;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;

/**
 * A {@link ChannelInboundHandler} for inspecting authentication access token for all
 * incoming HTTP requests to the router.
 */
public class AuthenticationHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationHandler.class);
  private static final Logger AUDIT_LOGGER = LoggerFactory.getLogger(Constants.Router.AUDIT_LOGGER_NAME);

  private final CConfiguration cConf;
  private final String realm;
  private final TokenValidator tokenValidator;
  private final Pattern bypassPattern;
  private final boolean auditLogEnabled;
  private final List<String> authServerURLs;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final AccessTokenTransformer tokenTransformer;

  public AuthenticationHandler(CConfiguration cConf, TokenValidator tokenValidator,
                               DiscoveryServiceClient discoveryServiceClient,
                               AccessTokenTransformer tokenTransformer) {
    this.cConf = cConf;
    this.realm = cConf.get(Constants.Security.CFG_REALM);
    this.tokenValidator = tokenValidator;
    this.bypassPattern = createBypassPattern(cConf);
    this.auditLogEnabled = cConf.getBoolean(Constants.Router.ROUTER_AUDIT_LOG_ENABLED);
    this.authServerURLs = getConfiguredAuthServerURLs(cConf);
    this.discoveryServiceClient = discoveryServiceClient;
    this.tokenTransformer = tokenTransformer;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof HttpRequest)) {
      ctx.fireChannelRead(msg);
      return;
    }

    HttpRequest request = (HttpRequest) msg;

    // Pass if security is bypassed or it has valid access token, process to the next handler
    if (isBypassed(request)) {
      ctx.fireChannelRead(msg);
      return;
    }
    TokenState tokenState = validateAccessToken(request, ctx.channel());
    if (tokenState.isValid()) {
      ctx.fireChannelRead(msg);
      return;
    }

    // Response with failure, plus optionally audit log
    try {
      HttpHeaders headers = new DefaultHttpHeaders();
      JsonObject jsonObject = new JsonObject();
      if (tokenState == TokenState.MISSING) {
        headers.add(HttpHeaderNames.WWW_AUTHENTICATE, String.format("Bearer realm=\"%s\"", realm));
        LOG.debug("Authentication failed due to missing token");
      } else {
        headers.add(HttpHeaderNames.WWW_AUTHENTICATE,
                    String.format("Bearer realm=\"%s\" error=\"invalid_token\"" +
                                    " error_description=\"%s\"", realm, tokenState.getMsg()));
        jsonObject.addProperty("error", "invalid_token");
        jsonObject.addProperty("error_description", tokenState.getMsg());
        LOG.debug("Authentication failed due to invalid token, reason={};", tokenState);
      }

      jsonObject.add("auth_uri", getAuthenticationURLs());

      ByteBuf content = Unpooled.copiedBuffer(jsonObject.toString(), StandardCharsets.UTF_8);
      HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                                                          HttpResponseStatus.UNAUTHORIZED, content);
      HttpUtil.setContentLength(response, content.readableBytes());
      response.headers().setAll(headers);
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json;charset=UTF-8");

      auditLogIfNeeded(request, response, ctx.channel());

      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }

  /**
   * Checks if the given request bypass access token validation.
   */
  private boolean isBypassed(HttpRequest request) {
    return bypassPattern != null && bypassPattern.matcher(request.uri()).matches();
  }

  /**
   * Validates the access token in authorization header.
   *
   * @param request the http request. The request headers will be modified if the validation succeeded to carry
   *                user information extracted from the token.
   * @return the {@link TokenState} indicating the result of the validation.
   */
  private TokenState validateAccessToken(HttpRequest request, Channel channel) {
    String auth = request.headers().get(HttpHeaderNames.AUTHORIZATION);

    /*
     * Parse the access token from authorization header.  The header will be in the form:
     *     Authorization: Bearer TOKEN
     *
     * where TOKEN is the base64 encoded serialized AccessToken instance.
     */
    String accessToken = null;
    if (auth != null) {
      int idx = auth.trim().indexOf(' ');
      if (idx < 0) {
        return TokenState.MISSING;
      }

      accessToken = auth.substring(idx + 1).trim();
    }
    TokenState state = tokenValidator.validate(accessToken);

    if (state.isValid()) {
      try {
        AccessTokenTransformer.AccessTokenIdentifierPair tokenPair = tokenTransformer.transform(accessToken);
        // Update message header
        request.headers().set(HttpHeaderNames.AUTHORIZATION,
                              "CDAP-verified " + tokenPair.getAccessTokenIdentifierStr());
        request.headers().set(Constants.Security.Headers.USER_ID,
                              tokenPair.getAccessTokenIdentifierObj().getUsername());
        String clientIP = Networks.getIP(channel.remoteAddress());
        if (clientIP != null) {
          request.headers().set(Constants.Security.Headers.USER_IP, clientIP);
        }
      } catch (Exception e) {
        // This shouldn't happen in normal case, since the token is already validated
        LOG.debug("Exception raised when getting token information from a validate token", e);
        return TokenState.INVALID;
      }
    }

    return state;
  }

  /**
   * Gets a {@link JsonArray} of url strings to the authentication server instances.
   */
  private JsonArray getAuthenticationURLs() {
    // If the auth server urls are known via configuration, just use it
    final JsonArray result = new JsonArray();

    if (!authServerURLs.isEmpty()) {
      for (String url : authServerURLs) {
        result.add(new JsonPrimitive(url));
      }
      return result;
    }

    // Use service discovery to get URLs of the auth servers
    final String protocol = getProtocol(cConf);
    final int port = getPort(cConf);

    ServiceDiscovered serviceDiscovered = discoveryServiceClient.discover(Constants.Service.EXTERNAL_AUTHENTICATION);
    addAuthServerUrls(serviceDiscovered, protocol, port, result);
    if (result.size() > 0) {
      return result;
    }

    // For bootstrapping, the service discovery takes time to fill in the cache from ZK, hence use a callback
    // and a timed future to get the result
    final SettableFuture<JsonArray> future = SettableFuture.create();
    Cancellable cancellable = serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
      @Override
      public void onChange(ServiceDiscovered serviceDiscovered) {
        addAuthServerUrls(serviceDiscovered, protocol, port, result);
        if (result.size() > 0) {
          future.set(result);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    try {
      return future.get(2, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      LOG.warn("No authentication server detected via service discovery");
      return result;
    } catch (Exception e) {
      // There shouldn't be other exception, hence just returning
      return result;
    } finally {
      cancellable.cancel();
    }
  }

  private void addAuthServerUrls(Iterable<Discoverable> discoverables, String protocol, int port, JsonArray result) {
    for (Discoverable discoverable : discoverables) {
      String url = String.format("%s://%s:%d/%s", protocol, discoverable.getSocketAddress().getHostName(), port,
                                 GrantAccessToken.Paths.GET_TOKEN);
      result.add(new JsonPrimitive(url));
    }
  }

  private void auditLogIfNeeded(HttpRequest request, HttpResponse response, Channel channel) {
    if (!auditLogEnabled) {
      return;
    }

    AuditLogEntry logEntry = new AuditLogEntry(request, Networks.getIP(channel.remoteAddress()));
    logEntry.setResponse(response);

    AUDIT_LOGGER.trace(logEntry.toString());
  }


  @Nullable
  private static Pattern createBypassPattern(CConfiguration cConf) {
    String pattern = cConf.get(Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX);
    if (pattern == null) {
      return null;
    }
    try {
      return Pattern.compile(pattern);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regular expression " + pattern + " for configuration "
                                           + Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX, e);
    }
  }

  private static List<String> getConfiguredAuthServerURLs(CConfiguration cConf) {
    List<String> urls = new ArrayList<>();
    // Get it from the configuration
    for (String url : cConf.getTrimmedStrings(Constants.Security.AUTH_SERVER_ANNOUNCE_URLS)) {
      urls.add(url + "/" + GrantAccessToken.Paths.GET_TOKEN);
    }
    return Collections.unmodifiableList(urls);
  }

  private static String getProtocol(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.SSL.EXTERNAL_ENABLED) ? "https" : "http";
  }

  private static int getPort(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.SSL.EXTERNAL_ENABLED)
      ? cConf.getInt(Constants.Security.AuthenticationServer.SSL_PORT)
      : cConf.getInt(Constants.Security.AUTH_SERVER_BIND_PORT);
  }
}
