/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import co.cask.cdap.common.logging.AuditLogContent;
import co.cask.cdap.common.logging.AuditLogEntry;
import co.cask.cdap.gateway.router.RouterAuditLookUp;
import co.cask.cdap.security.auth.AccessTokenTransformer;
import co.cask.cdap.security.auth.TokenState;
import co.cask.cdap.security.auth.TokenValidator;
import co.cask.cdap.security.server.GrantAccessToken;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ning.org.jboss.netty.handler.codec.http.HttpConstants;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferIndexFinder;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Security handler that intercept HTTP message and validates the access token in
 * header Authorization field.
 */
public class SecurityAuthenticationHttpHandler extends SimpleChannelHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityAuthenticationHttpHandler.class);
  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("http-access");
  private static final Set AUDIT_LOG_LOOKUP_METHOD = ImmutableSet.of(HttpMethod.PUT, HttpMethod.DELETE,
                                                                     HttpMethod.POST);
  private static final RouterAuditLookUp AUDIT_LOOK_UP = RouterAuditLookUp.getAuditLookUp();

  private final TokenValidator tokenValidator;
  private final AccessTokenTransformer accessTokenTransformer;
  private final Iterable<Discoverable> discoverables;
  private final CConfiguration configuration;
  private final String realm;
  private final Pattern bypassPattern;

  public SecurityAuthenticationHttpHandler(String realm, TokenValidator tokenValidator,
                                           CConfiguration configuration,
                                           AccessTokenTransformer accessTokenTransformer,
                                           DiscoveryServiceClient discoveryServiceClient) {
    this.realm = realm;
    this.tokenValidator = tokenValidator;
    this.accessTokenTransformer = accessTokenTransformer;
    this.discoverables = discoveryServiceClient.discover(Constants.Service.EXTERNAL_AUTHENTICATION);
    this.configuration = configuration;
    this.bypassPattern = createMatcher(configuration.get(Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX));
  }

  private Pattern createMatcher(String s) {
    if (s == null) {
      return null;
    }
    try {
      return Pattern.compile(s);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(String.format("Invalid regular expression for %s",
                                                       Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX), e);
    }
  }

  private boolean matchBypassPattern(HttpRequest req) {
    return bypassPattern != null && bypassPattern.matcher(req.getUri()).matches();
  }

  /**
   * Intercepts the HttpMessage for getting the access token in authorization header
   *
   * @param ctx channel handler context delegated from MessageReceived callback
   * @param msg intercepted HTTP message
   * @param inboundChannel
   * @return {@code true} if the HTTP message has valid Access token
   * @throws Exception
   */
  private boolean validateSecuredInterception(ChannelHandlerContext ctx, HttpRequest msg,
                                              Channel inboundChannel, AuditLogEntry logEntry) throws Exception {
    String auth = msg.getHeader(HttpHeaders.Names.AUTHORIZATION);
    String accessToken = null;

    /*
     * Parse the access token from authorization header.  The header will be in the form:
     *     Authorization: Bearer ACCESSTOKEN
     *
     * where ACCESSTOKEN is the base64 encoded serialized AccessToken instance.
     */
    if (auth != null) {
      int spIndex = auth.trim().indexOf(' ');
      if (spIndex != -1) {
        accessToken = auth.substring(spIndex + 1).trim();
      }
    }

    HttpMethod httpMethod = msg.getMethod();
    String uri = msg.getUri();
    logEntry.setClientIP(((InetSocketAddress) ctx.getChannel().getRemoteAddress()).getAddress());
    logEntry.setRequestLine(httpMethod, uri, msg.getProtocolVersion());

    TokenState tokenState = tokenValidator.validate(accessToken);
    if (!tokenState.isValid()) {
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
      logEntry.setResponseCode(HttpResponseStatus.UNAUTHORIZED.getCode());

      JsonObject jsonObject = new JsonObject();
      if (tokenState == TokenState.MISSING) {
        httpResponse.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE,
                               String.format("Bearer realm=\"%s\"", realm));
        LOG.debug("Authentication failed due to missing token");

      } else {
        httpResponse.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE,
                               String.format("Bearer realm=\"%s\" error=\"invalid_token\"" +
                                               " error_description=\"%s\"", realm, tokenState.getMsg()));
        jsonObject.addProperty("error", "invalid_token");
        jsonObject.addProperty("error_description", tokenState.getMsg());
        LOG.debug("Authentication failed due to invalid token, reason={};", tokenState);
      }
      JsonArray externalAuthenticationURIs = new JsonArray();

      // Waiting for service to get discovered
      stopWatchWait(externalAuthenticationURIs);

      jsonObject.add("auth_uri", externalAuthenticationURIs);

      ChannelBuffer content = ChannelBuffers.wrappedBuffer(jsonObject.toString().getBytes(Charsets.UTF_8));
      httpResponse.setContent(content);
      int contentLength = content.readableBytes();
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, contentLength);
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json;charset=UTF-8");
      logEntry.setResponseContentLength(new Long(contentLength));
      ChannelFuture writeFuture = Channels.future(inboundChannel);
      Channels.write(ctx, writeFuture, httpResponse);
      writeFuture.addListener(ChannelFutureListener.CLOSE);
      return false;
    } else {
      AccessTokenTransformer.AccessTokenIdentifierPair accessTokenIdentifierPair =
        accessTokenTransformer.transform(accessToken);
      AuditLogContent auditLogContent =
        AUDIT_LOG_LOOKUP_METHOD.contains(httpMethod)
          ? AUDIT_LOOK_UP.getAuditLogContent(msg.getUri(), httpMethod) : null;
      if (auditLogContent != null) {
        List<String> headerNames = auditLogContent.getHeaderNames();
        if (!headerNames.isEmpty()) {
          Map<String, String> headers = new HashMap<>();
          for (String headerName : headerNames) {
            headers.put(headerName, msg.getHeader(headerName));
          }
          logEntry.setHeaders(headers);
        }
        if (auditLogContent.isLogRequestBody()) {
          ChannelBuffer body = msg.getContent();
          if (body.readable()) {
            logEntry.setRequestBody(body.toString(Charsets.UTF_8));
          }
        }
        logEntry.setLogResponseBody(auditLogContent.isLogResponsebody());
      }

      logEntry.setUserName(accessTokenIdentifierPair.getAccessTokenIdentifierObj().getUsername());
      msg.setHeader(HttpHeaders.Names.AUTHORIZATION,
                    "CDAP-verified " + accessTokenIdentifierPair.getAccessTokenIdentifierStr());
      msg.setHeader(Constants.Security.Headers.USER_ID,
                    accessTokenIdentifierPair.getAccessTokenIdentifierObj().getUsername());
      msg.setHeader(Constants.Security.Headers.USER_IP,
                    ((InetSocketAddress) ctx.getChannel().getRemoteAddress()).getAddress().getHostAddress());
      return true;
    }
  }

  /**
   * @param externalAuthenticationURIs the list that should be populated with discovered with
   *                                   external auth servers URIs
   * @throws Exception
   */
  private void stopWatchWait(JsonArray externalAuthenticationURIs) throws Exception {
    boolean done = false;
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    String[] announceURLs = configuration.getTrimmedStrings(Constants.Security.AUTH_SERVER_ANNOUNCE_URLS);
    // If the announceURLs is set in configuration, add the URL's to the list
    if (announceURLs.length > 0) {
      for (String url : announceURLs) {
        String urlWithToken = String.format("%s/%s", url, GrantAccessToken.Paths.GET_TOKEN);
        externalAuthenticationURIs.add(new JsonPrimitive(urlWithToken));
      }
      return;
    }

    String protocol;
    int port;
    if (configuration.getBoolean(Constants.Security.SSL.EXTERNAL_ENABLED)) {
      protocol = "https";
      port = configuration.getInt(Constants.Security.AuthenticationServer.SSL_PORT);
    } else {
      protocol = "http";
      port = configuration.getInt(Constants.Security.AUTH_SERVER_BIND_PORT);
    }
    String announceAddress = configuration.get(Constants.Security.AUTH_SERVER_ANNOUNCE_ADDRESS_DEPRECATED);
    if (announceAddress != null) {
      String url;
      if (announceAddress.matches(".+:[0-9]+")) {
        // announceAddress already contains port
        url = String.format("%s://%s/%s", protocol, announceAddress, GrantAccessToken.Paths.GET_TOKEN);
      } else {
        url = String.format("%s://%s:%d/%s", protocol, announceAddress, port, GrantAccessToken.Paths.GET_TOKEN);
      }
      externalAuthenticationURIs.add(new JsonPrimitive(url));
      return;
    }
    do {
      for (Discoverable d : discoverables) {
        String url = String.format("%s://%s:%d/%s", protocol, d.getSocketAddress().getHostName(), port,
                                   GrantAccessToken.Paths.GET_TOKEN);
        externalAuthenticationURIs.add(new JsonPrimitive(url));
        done = true;
      }
      if (!done) {
        TimeUnit.MILLISECONDS.sleep(200);
      }
    } while (!done && stopwatch.elapsedTime(TimeUnit.SECONDS) < 2L);
  }


  @Override
  public void messageReceived(ChannelHandlerContext ctx, final MessageEvent event) throws Exception {
    Object msg = event.getMessage();
    if (!(msg instanceof HttpRequest)) {
      super.messageReceived(ctx, event);
    } else {
      AuditLogEntry logEntry = new AuditLogEntry();
      ctx.setAttachment(logEntry);
      HttpRequest req = (HttpRequest) msg;
      if (matchBypassPattern(req) || validateSecuredInterception(ctx, req, event.getChannel(), logEntry)) {
        Channels.fireMessageReceived(ctx, msg, event.getRemoteAddress());
      }
      // we write the response directly for authentication failure, so nothing to do for else
    }
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    AuditLogEntry logEntry = getLogEntry(ctx);
    boolean isLogResponseBody = logEntry.isLogResponseBody();
    Object message = e.getMessage();
    if (message instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) message;
      logEntry.setResponseCode(response.getStatus().getCode());
      if (isLogResponseBody) {
        ChannelBuffer body = response.getContent();
        if (body.readable()) {
          logEntry.setResponseBody(body.toString(Charsets.UTF_8));
        }
      }
      if (response.containsHeader(HttpHeaders.Names.CONTENT_LENGTH)) {
        String lengthString = response.getHeader(HttpHeaders.Names.CONTENT_LENGTH);
        try {
          logEntry.setResponseContentLength(Long.valueOf(lengthString));
        } catch (NumberFormatException nfe) {
          LOG.warn("Invalid value for content length in HTTP response message: {}", lengthString, nfe);
        }
      }
    } else if (message instanceof ChannelBuffer) {
      // for chunked responses the response code will only be present on the first chunk
      // so we only look for it the first time around
      ChannelBuffer channelBuffer = (ChannelBuffer) message;
      if (logEntry.getResponseCode() == null) {
        logEntry.setResponseCode(findResponseCode(channelBuffer));
        if (logEntry.getResponseCode() != null) {
          // we currently only look for a Content-Length header in the first buffer on an HTTP response
          // this is a limitation of the implementation that simplifies header parsing
          logEntry.setResponseContentLength(findContentLength(channelBuffer));
          if (isLogResponseBody) {
            logEntry.setResponseBody(findResponseBody(channelBuffer, true));
          }
        }
      } else if (isLogResponseBody) {
        logEntry.appendResponseBody(findResponseBody(channelBuffer, false));
      }
    } else {
      LOG.debug("Unhandled response message type: {}", message.getClass());
    }
    super.writeRequested(ctx, e);
  }

  @Override
  public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
    AuditLogEntry logEntry = getLogEntry(ctx);
    if (!logEntry.isLogged()) {
      AUDIT_LOG.trace(logEntry.toString());
      logEntry.setLogged(true);
    }
  }

  private Integer findResponseCode(ChannelBuffer buffer) {
    Integer responseCode = null;

    // we assume that the response code should follow the first space in the first line of the response
    int indx = buffer.indexOf(buffer.readerIndex(), buffer.writerIndex(), ChannelBufferIndexFinder.LINEAR_WHITESPACE);
    if (indx >= 0 && indx < buffer.writerIndex() - 4) {
      String codeString = buffer.toString(indx, 4, Charsets.UTF_8).trim();
      try {
        responseCode = Integer.valueOf(codeString);
      } catch (NumberFormatException nfe) {
        LOG.warn("Invalid value for HTTP response code: {}", codeString, nfe);
      }
    } else {
      LOG.debug("Invalid index for space in response: index={}, buffer size={}", indx, buffer.readableBytes());
    }
    return responseCode;
  }

  private Long findContentLength(ChannelBuffer buffer) {
    Long contentLength = null;
    int bufferEnd = buffer.writerIndex();
    int index = buffer.indexOf(buffer.readerIndex(), bufferEnd, CONTENT_LENGTH_FINDER);
    if (index >= 0) {
      // find the following ':'
      int colonIndex = buffer.indexOf(index, bufferEnd, HttpConstants.COLON);
      int eolIndex = buffer.indexOf(index, bufferEnd, ChannelBufferIndexFinder.CRLF);
      if (colonIndex > 0 && colonIndex < eolIndex) {
        String lengthString = buffer.toString(colonIndex + 1, eolIndex - (colonIndex + 1), Charsets.UTF_8).trim();
        try {
          contentLength = Long.valueOf(lengthString);
        } catch (NumberFormatException nfe) {
          LOG.warn("Invalid value for content length in HTTP response message: {}", lengthString, nfe);
        }
      }
    }
    return contentLength;
  }

  private AuditLogEntry getLogEntry(ChannelHandlerContext ctx) {
    Object entryObject = ctx.getAttachment();
    AuditLogEntry logEntry;
    if (entryObject != null && entryObject instanceof AuditLogEntry) {
      logEntry = (AuditLogEntry) entryObject;
    } else {
      logEntry = new AuditLogEntry();
      ctx.setAttachment(logEntry);
    }
    return logEntry;
  }

  private String findResponseBody(ChannelBuffer channelBuffer, boolean isFirstChunk) {
    if (!isFirstChunk) {
      return channelBuffer.readable() ? channelBuffer.toString(Charsets.UTF_8) : "";
    }
    int bufferEnd = channelBuffer.writerIndex();
    int start = channelBuffer.indexOf(channelBuffer.readerIndex(), bufferEnd, TWO_SEPERATOR_FINDER);

    if (start >= 0) {
      // The http message separator is CRLF, which is "\r\n", two CRLF will be 4 characters.
      return channelBuffer.toString(start + 4, bufferEnd - start - 4, Charsets.UTF_8).trim();
    }
    return "";
  }

  private static final ChannelBufferIndexFinder CONTENT_LENGTH_FINDER = new ChannelBufferIndexFinder() {
    private byte[] headerName = HttpHeaders.Names.CONTENT_LENGTH.getBytes(Charsets.UTF_8);

    @Override
    public boolean find(ChannelBuffer buffer, int guessedIndex) {
      if (buffer.capacity() - guessedIndex < headerName.length) {
        return false;
      }

      for (int i = 0; i < headerName.length; i++) {
        if (headerName[i] != buffer.getByte(guessedIndex + i)) {
          return false;
        }
      }
      return true;
    }
  };

  private static final ChannelBufferIndexFinder TWO_SEPERATOR_FINDER = new ChannelBufferIndexFinder() {
    private byte[] lineSeperator  = "\r\n\r\n".getBytes(Charsets.UTF_8);

    @Override
    public boolean find(ChannelBuffer buffer, int guessedIndex) {
      if (buffer.capacity() - guessedIndex < lineSeperator.length) {
        return false;
      }

      for (int i = 0; i < lineSeperator.length; i++) {
        if (lineSeperator[i] != buffer.getByte(guessedIndex + i)) {
          return false;
        }
      }
      return true;
    }
  };
}
