package com.continuuity.gateway.router.handlers;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.security.auth.AccessTokenTransformer;
import com.continuuity.security.auth.TokenState;
import com.continuuity.security.auth.TokenValidator;
import com.continuuity.security.server.GrantAccessToken;
import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
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
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Security handler that intercept HTTP message and validates the access token in
 * header Authorization field.
 */
public class SecurityAuthenticationHttpHandler extends SimpleChannelHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SecurityAuthenticationHttpHandler.class);
  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("http-access");

  private final TokenValidator tokenValidator;
  private final AccessTokenTransformer accessTokenTransformer;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Iterable<Discoverable> discoverables;
  private final CConfiguration configuration;
  private final String realm;
  private final DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");


  public SecurityAuthenticationHttpHandler(String realm, TokenValidator tokenValidator,
                                           CConfiguration configuration,
                                           AccessTokenTransformer accessTokenTransformer,
                                           DiscoveryServiceClient discoveryServiceClient) {
    this.realm = realm;
    this.tokenValidator = tokenValidator;
    this.accessTokenTransformer = accessTokenTransformer;
    this.discoveryServiceClient = discoveryServiceClient;
    this.discoverables = discoveryServiceClient.discover(Constants.Service.EXTERNAL_AUTHENTICATION);
    this.configuration = configuration;
  }

  /**
   * Intercepts the HttpMessage for getting the access token in authorization header
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

    logEntry.clientIP = ((InetSocketAddress) ctx.getChannel().getRemoteAddress()).getAddress();
    logEntry.requestLine = msg.getMethod() + " " + msg.getUri() + " " + msg.getProtocolVersion();

    TokenState tokenState = tokenValidator.validate(accessToken);
    if (!tokenState.isValid()) {
      HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
      logEntry.responseCode = HttpResponseStatus.UNAUTHORIZED.getCode();

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

      //Waiting for service to get discovered
      stopWatchWait(externalAuthenticationURIs);

      jsonObject.add("auth_uri", externalAuthenticationURIs);

      ChannelBuffer content = ChannelBuffers.wrappedBuffer(jsonObject.toString().getBytes(Charsets.UTF_8));
      httpResponse.setContent(content);
      int contentLength = content.readableBytes();
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, contentLength);
      httpResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json;charset=UTF-8");
      logEntry.responseContentLength = new Long(contentLength);
      ChannelFuture writeFuture = Channels.future(inboundChannel);
      Channels.write(ctx, writeFuture, httpResponse);
      writeFuture.addListener(ChannelFutureListener.CLOSE);
      return false;
    } else {
      AccessTokenTransformer.AccessTokenIdentifierPair accessTokenIdentifierPair =
        accessTokenTransformer.transform(accessToken);
      logEntry.userName = accessTokenIdentifierPair.getAccessTokenIdentifierObj().getUsername();
      msg.setHeader(HttpHeaders.Names.AUTHORIZATION,
                    "Reactor-verified " + accessTokenIdentifierPair.getAccessTokenIdentifierStr());
      return true;
    }
  }

  /**
   *
   * @param externalAuthenticationURIs the list that should be populated with discovered with
   *                                   external auth servers URIs
   * @throws Exception
   */
  private void stopWatchWait(JsonArray externalAuthenticationURIs) throws Exception {
    boolean done = false;
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    String protocol;
    int port;
    if (configuration.getBoolean(Constants.Security.SSL_ENABLED)) {
      protocol = "https";
      port = configuration.getInt(Constants.Security.AUTH_SERVER_SSL_PORT);
    } else {
      protocol = "http";
      port = configuration.getInt(Constants.Security.AUTH_SERVER_PORT);
    }

    do {
      for (Discoverable d : discoverables)  {
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
      if (validateSecuredInterception(ctx, (HttpRequest) msg, event.getChannel(), logEntry)) {
        Channels.fireMessageReceived(ctx, msg, event.getRemoteAddress());
      } else {
        // we write the response directly for authentication failure, so nothing to do
        return;
      }
    }
  }

  @Override
  public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
    AuditLogEntry logEntry = getLogEntry(ctx);
    Object message = e.getMessage();
    if (message instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) message;
      logEntry.responseCode = response.getStatus().getCode();
      if (response.containsHeader(HttpHeaders.Names.CONTENT_LENGTH)) {
        String lengthString = response.getHeader(HttpHeaders.Names.CONTENT_LENGTH);
        try {
          logEntry.responseContentLength = Long.valueOf(lengthString);
        } catch (NumberFormatException nfe) {
          LOG.warn("Invalid value for content length in HTTP response message: {}", lengthString, nfe);
        }
      }
    } else if (message instanceof ChannelBuffer) {
      // for chunked responses the response code will only be present on the first chunk
      // so we only look for it the first time around
      if (logEntry.responseCode == null) {
        ChannelBuffer channelBuffer = (ChannelBuffer) message;
        logEntry.responseCode = findResponseCode(channelBuffer);
        if (logEntry.responseCode != null) {
          // we currently only look for a Content-Length header in the first buffer on an HTTP response
          // this is a limitation of the implementation that simplifies header parsing
          logEntry.responseContentLength = findContentLength(channelBuffer);
        }
      }
    } else {
      LOG.debug("Unhandled response message type: {}", message.getClass());
    }
    super.writeRequested(ctx, e);
  }

  @Override
  public void writeComplete(ChannelHandlerContext ctx, WriteCompletionEvent e) throws Exception {
    AuditLogEntry logEntry = getLogEntry(ctx);
    if (!logEntry.logged) {
      AUDIT_LOG.trace(logEntry.toString());
      logEntry.logged = true;
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

  private final class AuditLogEntry {
    /** Each audit log field will default to "-" if the field is missing or not supported. */
    private static final String DEFAULT_VALUE = "-";

    /** Indicates whether this entry has already been logged. */
    private boolean logged;

    private InetAddress clientIP;
    private String userName;
    private Date date;
    private String requestLine;
    private Integer responseCode;
    private Long responseContentLength;

    public AuditLogEntry() {
      this.date = new Date();
    }

    public String toString() {
      return String.format("%s %s [%s] \"%s\" %s %s",
                           clientIP != null ? clientIP.getHostAddress() : DEFAULT_VALUE,
                           fieldOrDefault(userName),
                           dateFormat.format(date),
                           fieldOrDefault(requestLine),
                           fieldOrDefault(responseCode),
                           fieldOrDefault(responseContentLength));
    }

    private String fieldOrDefault(Object field) {
      return field == null ? DEFAULT_VALUE : field.toString();
    }
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
}
