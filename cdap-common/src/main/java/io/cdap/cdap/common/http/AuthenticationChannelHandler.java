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

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An UpstreamHandler that verifies the userId in a request header and updates the {@code SecurityRequestContext}.
 */
public class AuthenticationChannelHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationChannelHandler.class);

  private static final String EMPTY_USER_ID = "CDAP-empty-user-id";
  private static final Credential EMPTY_USER_CREDENTIAL = new Credential("CDAP-empty-user-credential",
                                                                         Credential.CredentialType.INTERNAL);
  private static final String EMPTY_USER_IP = "CDAP-empty-user-ip";

  private final boolean internalAuthEnabled;

  private String currentUserID;
  private Credential currentUserCredential;
  private String currentUserIP;

  public AuthenticationChannelHandler(boolean internalAuthEnabled) {
    this.internalAuthEnabled = internalAuthEnabled;
  }

  /**
   * Decode the AccessTokenIdentifier passed as a header and set it in a ThreadLocal.
   * Returns a 401 if the identifier is malformed.
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    SecurityRequestContext.reset();
    resetCurrentUserInfo();

    if (msg instanceof HttpRequest) {
      // TODO: authenticate the user using user id - CDAP-688
      HttpRequest request = (HttpRequest) msg;
      String userID = request.headers().get(Constants.Security.Headers.USER_ID);
      if (userID != null) {
        currentUserID = userID;
      }
      String userIP = request.headers().get(Constants.Security.Headers.USER_IP);
      if (userIP != null) {
        currentUserIP = userIP;
      }
      String authHeader = request.headers().get(Constants.Security.Headers.RUNTIME_TOKEN);
      if (authHeader != null) {
        int idx = authHeader.trim().indexOf(' ');
        if (idx < 0) {
          LOG.error("Invalid Authorization header format for {}@{}", currentUserID, currentUserIP);
          if (internalAuthEnabled) {
            throw new IllegalArgumentException("Invalid Authorization header format");
          }
        } else {
          String credentialTypeStr = authHeader.substring(0, idx);
          try {
            Credential.CredentialType credentialType = Credential.CredentialType.fromQualifiedName(credentialTypeStr);
            String credentialValue = authHeader.substring(idx + 1).trim();
            currentUserCredential = new Credential(credentialValue, credentialType);
            SecurityRequestContext.setUserCredential(currentUserCredential);
          } catch (IllegalArgumentException e) {
            LOG.error("Invalid credential type in Authorization header: {}", credentialTypeStr);
            throw e;
          }
        }
      }
      LOG.trace("Got user ID '{}' user IP '{}' from IP '{}' and authorization header length '{}'",
                userID, userIP, ctx.channel().remoteAddress(),
                authHeader == null ? "NULL" : String.valueOf(authHeader.length()));
      SecurityRequestContext.setUserId(currentUserID);
      SecurityRequestContext.setUserCredential(currentUserCredential);
      SecurityRequestContext.setUserIP(currentUserIP);
    } else if (msg instanceof HttpContent) {
      SecurityRequestContext.setUserId(currentUserID);
      SecurityRequestContext.setUserCredential(currentUserCredential);
      SecurityRequestContext.setUserIP(currentUserIP);
    }

    try {
      ctx.fireChannelRead(msg);
    } finally {
      SecurityRequestContext.reset();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.error("Got exception: {}", cause.getMessage(), cause);
    // TODO: add WWW-Authenticate header for 401 response -  REACTOR-900
    HttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
    HttpUtil.setContentLength(response, 0);
    HttpUtil.setKeepAlive(response, false);
    ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
  }

  private void resetCurrentUserInfo() {
    if (internalAuthEnabled) {
      currentUserID = EMPTY_USER_ID;
      currentUserCredential = EMPTY_USER_CREDENTIAL;
      currentUserIP = EMPTY_USER_IP;
    } else {
      currentUserID = null;
      currentUserCredential = null;
      currentUserIP = null;
    }
  }
}
