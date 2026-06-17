/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.auditlogging.AuditLogSetterHook;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.metrics.MetricsReporterHook;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.cdap.features.Feature;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutor;

import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Provides a {@link io.cdap.http.NettyHttpService.Builder} that has common settings built-in.
 */
public class CommonNettyHttpServiceBuilder extends NettyHttpService.Builder {

  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CommonNettyHttpServiceBuilder.class);
  public static final String AUTHENTICATOR_NAME = "authenticator";

  private ChannelPipelineModifier pipelineModifier;
  private ChannelPipelineModifier additionalModifier;

  public CommonNettyHttpServiceBuilder(CConfiguration cConf, String serviceName,
      MetricsCollectionService metricsCollectionService, AuditLogWriter auditLogWriter) {
    super(serviceName);
    if (cConf.getBoolean(Constants.Security.ENABLED)) {
      FeatureFlagsProvider featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
      boolean auditLoggingEnabled = Feature.DATAPLANE_AUDIT_LOGGING.isEnabled(featureFlagsProvider) ;

      pipelineModifier = new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          pipeline.addFirst("socket-boundary-logger", new io.netty.channel.ChannelOutboundHandlerAdapter() {
            @Override
            public void write(io.netty.channel.ChannelHandlerContext ctx, Object msg, io.netty.channel.ChannelPromise promise) throws Exception {
              if (msg instanceof io.netty.buffer.ByteBuf) {
                io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) msg;
                LOG.info("AppFabric JVM [Ch:{}]: Writing {} raw encrypted binary bytes directly to physical OS socket.", ctx.channel().id().asShortText(), buf.readableBytes());
              }
              ctx.write(msg, promise);
            }
          });
          // Adds the AuthenticationChannelHandler before the dispatcher, using the same
          // EventExecutor to make sure they get invoked from the same thread
          // This is needed before we use a InheritableThreadLocal in SecurityRequestContext
          // to remember the user id.
          EventExecutor executor = pipeline.context("dispatcher").executor();
          pipeline.addBefore(executor, "dispatcher", AUTHENTICATOR_NAME,
                             new AuthenticationChannelHandler(cConf.getBoolean(Constants.Security
                                 .INTERNAL_AUTH_ENABLED), auditLoggingEnabled, auditLogWriter));
          pipeline.addBefore(executor, "dispatcher", "response-logger", new io.netty.channel.ChannelDuplexHandler() {
            @Override
            public void write(io.netty.channel.ChannelHandlerContext ctx, Object msg, io.netty.channel.ChannelPromise promise) throws Exception {
              String chId = ctx.channel().id().asShortText();
              if (msg instanceof io.netty.handler.codec.http.HttpResponse) {
                io.netty.handler.codec.http.HttpResponse res = (io.netty.handler.codec.http.HttpResponse) msg;
                LOG.info("AppFabric Netty [Ch:{}]: Writing HTTP response headers to channel: status={}", chId, res.status());
              }
              if (msg instanceof io.netty.handler.codec.http.LastHttpContent) {
                LOG.info("AppFabric Netty [Ch:{}]: Writing LastHttpContent completion to socket channel.", chId);
                ctx.write(msg, promise).addListener(new io.netty.channel.ChannelFutureListener() {
                  @Override
                  public void operationComplete(io.netty.channel.ChannelFuture future) {
                    if (future.isSuccess()) {
                      LOG.info("AppFabric Netty [Ch:{}]: Response successfully written to OS TCP socket buffer.", chId);
                    } else {
                      LOG.error("AppFabric Netty [Ch:{}]: Failed to write response bytes to socket! Error: {}", chId, future.cause() == null ? "unknown" : future.cause().getMessage());
                    }
                  }
                });
                return;
              }
              ctx.write(msg, promise);
            }
          });
        }
      };
    }
    this.setExceptionHandler(new HttpExceptionHandler());
    this.setHandlerHooks(Collections.unmodifiableList(
      Arrays.asList(new MetricsReporterHook(cConf, metricsCollectionService, serviceName),
                    new AuditLogSetterHook(cConf, serviceName))));
  }

  /**
   * Sets pipeline modifier, preserving the security one installed in constructor.
   */
  @Override
  public NettyHttpService.Builder setChannelPipelineModifier(
      ChannelPipelineModifier additionalPipelineModifier) {
    additionalModifier = additionalPipelineModifier;
    return this;
  }

  /**
   * Sets a pipeline modifier replacing the security one installed in constructor.
   */
  public NettyHttpService.Builder replaceDefaultChannelPipelineModifier(
      ChannelPipelineModifier channelPipelineModifier) {

    pipelineModifier = channelPipelineModifier;
    return this;
  }

  public NettyHttpService.Builder addChannelPipelineModifier(
      ChannelPipelineModifier additionalPipelineModifier) {
    additionalModifier = combine(additionalModifier, additionalPipelineModifier);
    return this;
  }

  @Override
  public NettyHttpService build() {
    ChannelPipelineModifier modifier = combine(pipelineModifier, additionalModifier);
    if (modifier != null) {
      super.setChannelPipelineModifier(modifier);
    }
    return super.build();
  }

  private ChannelPipelineModifier combine(@Nullable ChannelPipelineModifier existing,
      @Nullable ChannelPipelineModifier additional) {
    if (existing == null) {
      return additional;
    }
    if (additional == null) {
      return existing;
    }
    return new ChannelPipelineModifier() {
      @Override
      public void modify(ChannelPipeline pipeline) {
        existing.modify(pipeline);
        additional.modify(pipeline);
      }
    };
  }
}
