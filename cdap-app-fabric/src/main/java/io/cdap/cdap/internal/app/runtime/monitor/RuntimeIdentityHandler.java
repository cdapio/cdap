/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.AuditLogEntry;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ChannelInboundHandler} for properly propagating runtime identity to calls to other
 * system services.
 */
public class RuntimeIdentityHandler extends ChannelInboundHandlerAdapter {

  private static final Logger AUDIT_LOGGER = LoggerFactory
      .getLogger(Constants.RuntimeMonitor.MONITOR_AUDIT_LOGGER_NAME);

  private static final String EMPTY_RUNTIME_TOKEN =
      String.format("%s %s", Credential.CREDENTIAL_TYPE_INTERNAL, "empty-runtime-token");

  private final boolean enforceAuthenticatedRequests;
  private final boolean auditLogEnabled;

  public RuntimeIdentityHandler(CConfiguration cConf) {
    this.enforceAuthenticatedRequests = SecurityUtil.isInternalAuthEnabled(cConf);
    this.auditLogEnabled = cConf.getBoolean(Constants.RuntimeMonitor.MONITOR_AUDIT_LOG_ENABLED);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof HttpRequest)) {
      ctx.fireChannelRead(msg);
      return;
    }

    HttpRequest request = (HttpRequest) msg;

    request.headers().remove(HttpHeaderNames.AUTHORIZATION);
    request.headers().set(Constants.Security.Headers.USER_ID,
        Constants.Security.Authentication.RUNTIME_IDENTITY);
    String clientIP = Networks.getIP(ctx.channel().remoteAddress());
    if (clientIP != null) {
      request.headers().set(Constants.Security.Headers.USER_IP, clientIP);
    } else {
      request.headers().remove(Constants.Security.Headers.USER_IP);
    }
    if (enforceAuthenticatedRequests && !request.headers()
        .contains(Constants.Security.Headers.RUNTIME_TOKEN)) {
      request.headers().set(Constants.Security.Headers.RUNTIME_TOKEN, EMPTY_RUNTIME_TOKEN);
    }
    ctx.fireChannelRead(msg);
  }

  private void auditLogIfNeeded(HttpRequest request, Channel channel, int code) {
    if (!auditLogEnabled) {
      return;
    }

    AuditLogEntry logEntry = new AuditLogEntry(request, Networks.getIP(channel.remoteAddress()));
    logEntry.setResponse(code, -1);

    AUDIT_LOGGER.trace(logEntry.toString());
  }
}
