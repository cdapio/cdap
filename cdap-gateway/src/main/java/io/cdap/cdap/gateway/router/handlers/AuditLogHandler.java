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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.AuditLogConfig;
import co.cask.cdap.common.logging.AuditLogEntry;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.gateway.router.RouterAuditLookUp;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * A {@link ChannelDuplexHandler} that captures {@link HttpRequest} and corresponding {@link HttpResponse}
 * over the forwarding connections from router to CDAP services (via the client bootstrap) for audit log purpose.
 * The router logic guarantees that the same forwarding connection won't have more than one request on the fly
 * (i.e. no HTTP pipeline), hence it's safe to use fields to remember the {@link AuditLogEntry}
 * and have it tied back with the {@link HttpRequest} when a {@link HttpResponse} is received.
 */
public class AuditLogHandler extends ChannelDuplexHandler {

  private static final Logger AUDIT_LOGGER = LoggerFactory.getLogger(Constants.Router.AUDIT_LOGGER_NAME);
  private static final Set<HttpMethod> AUDIT_LOG_LOOKUP_METHOD = ImmutableSet.of(HttpMethod.PUT, HttpMethod.DELETE,
                                                                                 HttpMethod.POST);
  private AuditLogEntry logEntry;
  private boolean logRequestBody;
  private boolean logResponseBody;

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    // When a request is forwarded to the internal CDAP service
    if (msg instanceof HttpRequest) {
      HttpRequest request = (HttpRequest) msg;

      // Extra configurations for audit log
      AuditLogConfig logConfig = AUDIT_LOG_LOOKUP_METHOD.contains(request.method())
        ? RouterAuditLookUp.getInstance().getAuditLogContent(request.uri(), request.method()) : null;

      if (logConfig == null) {
        logEntry = new AuditLogEntry(request, Networks.getIP(ctx.channel().remoteAddress()));
      } else {
        logEntry = new AuditLogEntry(request, Networks.getIP(ctx.channel().remoteAddress()),
                                     logConfig.getHeaderNames());
        logRequestBody = logConfig.isLogRequestBody();
        logResponseBody = logConfig.isLogResponseBody();
      }
    } else if (msg instanceof HttpContent && logEntry != null) {
      ByteBuf content = ((HttpContent) msg).content();
      if (logRequestBody && content.isReadable()) {
        logEntry.appendRequestBody(content.toString(StandardCharsets.UTF_8));
      }
    }

    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    if (logEntry == null) {
      ctx.write(msg, promise);
      return;
    }

    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;
      logEntry.setResponse(response);

      // If no need to log the response body, we can emit the audit log
      if (!logResponseBody) {
        emitAuditLog();
      }
    } else if (msg instanceof HttpContent && logResponseBody) {
      ByteBuf content = ((HttpContent) msg).content();
      if (content.isReadable()) {
        logEntry.appendResponseBody(content.toString(StandardCharsets.UTF_8));
      }

      // If need to log the response body, emit the audit log when all response contents are received
      if (msg instanceof LastHttpContent) {
        emitAuditLog();
      }
    }

    ctx.write(msg, promise);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (logEntry != null) {
      emitAuditLog();
    }
    ctx.fireChannelInactive();
  }

  private void emitAuditLog() {
    try {
      AUDIT_LOGGER.trace(logEntry.toString());
    } finally {
      logEntry = null;
    }
  }
}
