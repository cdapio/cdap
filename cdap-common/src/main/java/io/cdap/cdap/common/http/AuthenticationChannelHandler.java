/*
 * Copyright © 2014-2021 Cask Data, Inc.
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

package io.cdap.cdap.common.http;

import io.cdap.cdap.api.auditlogging.AuditLogPublisher;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

/**
 * An UpstreamHandler that verifies the userId in a request header and updates the {@code
 * SecurityRequestContext}.
 */
public class AuthenticationChannelHandler extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationChannelHandler.class);

  private static final String EMPTY_USER_ID = "CDAP-empty-user-id";
  private static final Credential EMPTY_USER_CREDENTIAL = new Credential(
      "CDAP-empty-user-credential",
      Credential.CredentialType.INTERNAL);
  private static final String EMPTY_USER_IP = "CDAP-empty-user-ip";

  private final boolean internalAuthEnabled;
  private final AuditLogPublisher auditLogPublisher;

  public AuthenticationChannelHandler(boolean internalAuthEnabled, AuditLogPublisher auditLogPublisher) {
    this.internalAuthEnabled = internalAuthEnabled;
    this.auditLogPublisher = auditLogPublisher;
  }

  /**
   * Decode the AccessTokenIdentifier passed as a header and set it in a ThreadLocal. Returns a 401
   * if the identifier is malformed.
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    SecurityRequestContext.reset();

    // Only set SecurityRequestContext for the HttpRequest but not for subsequence chunks.
    // We cannot set a default/placeholder value until CDAP-18773
    // is fixed since we may perform auth checks in the thread processing the last chunk.
    if (msg instanceof HttpRequest) {

      String currentUserId = null;
      Credential currentUserCredential = null;
      String currentUserIp = null;

      if (internalAuthEnabled) {
        // When internal auth is enabled, all requests should typically have user id and credential
        // associated with them, for instance, end user credential for user originated ones and
        // internal system credential for system originated requests. If there is none, set
        // default empty user id and credential.
        currentUserId = EMPTY_USER_ID;
        currentUserCredential = EMPTY_USER_CREDENTIAL;
        currentUserIp = EMPTY_USER_IP;
      }
      // TODO: authenticate the user using user id - CDAP-688
      HttpRequest request = (HttpRequest) msg;
      String userId = request.headers().get(Constants.Security.Headers.USER_ID);
      if (userId != null) {
        currentUserId = userId;
      }
      String userIp = request.headers().get(Constants.Security.Headers.USER_IP);
      if (userIp != null) {
        currentUserIp = userIp;
      }
      String authHeader = request.headers().get(Constants.Security.Headers.RUNTIME_TOKEN);
      if (authHeader != null) {
        int idx = authHeader.trim().indexOf(' ');
        if (idx < 0) {
          LOG.error("Invalid Authorization header format for {}@{}", currentUserId, currentUserIp);
          if (internalAuthEnabled) {
            throw new UnauthenticatedException("Invalid Authorization header format");
          }
        } else {
          String credentialTypeStr = authHeader.substring(0, idx);
          try {
            Credential.CredentialType credentialType = Credential.CredentialType.fromQualifiedName(
                credentialTypeStr);
            String credentialValue = authHeader.substring(idx + 1).trim();
            currentUserCredential = new Credential(credentialValue, credentialType);
            SecurityRequestContext.setUserCredential(currentUserCredential);
          } catch (IllegalArgumentException e) {
            LOG.error("Invalid credential type in Authorization header: {}", credentialTypeStr);
            throw new UnauthenticatedException(e);
          }
        }
      }
      LOG.trace("Got user ID '{}' user IP '{}' from IP '{}' and authorization header length '{}'",
          userId, userIp, ctx.channel().remoteAddress(),
          authHeader == null ? "NULL" : authHeader.length());
      SecurityRequestContext.setUserId(currentUserId);
      SecurityRequestContext.setUserCredential(currentUserCredential);
      SecurityRequestContext.setUserIp(currentUserIp);
    }

    LOG.warn("SANKET_LOG : read1");

    try {
      ctx.fireChannelRead(msg);
    } finally {
      SecurityRequestContext.reset();
    }
  }

  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (msg instanceof HttpResponse) {
      LOG.warn("SANKET_LOG : HttpResponse " + (HttpResponse) msg);
      auditLogPublisher.publish(SecurityRequestContext.getAuditLogQueue());
    }
    super.write(ctx, msg, promise);
  }

    @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.error("Got exception: {}", cause.getMessage(), cause);
    // TODO: add WWW-Authenticate header for 401 response -  REACTOR-900
    HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
        HttpResponseStatus.UNAUTHORIZED);
    HttpUtil.setContentLength(response, 0);
    HttpUtil.setKeepAlive(response, false);
    ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }
}
