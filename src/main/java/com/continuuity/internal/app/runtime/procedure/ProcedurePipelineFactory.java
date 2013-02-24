package com.continuuity.internal.app.runtime.procedure;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;

/**
 *
 */
final class ProcedurePipelineFactory implements ChannelPipelineFactory {

  private final ExecutionHandler executionHandler;
  private final ProcedureDispatcher dispatcher;
  private final ChannelUpstreamHandler connectionTracker;

  ProcedurePipelineFactory(ExecutionHandler executionHandler,
                           HandlerMethodFactory handlerMethodFactory,
                           final ChannelGroup channelGroup) {
    this.executionHandler = executionHandler;
    this.dispatcher = new ProcedureDispatcher(handlerMethodFactory);
    this.connectionTracker = new SimpleChannelUpstreamHandler() {
      @Override
      public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        channelGroup.add(e.getChannel());
        super.handleUpstream(ctx, e);
      }
    };
  }

  @Override
  public ChannelPipeline getPipeline() throws Exception {
    ChannelPipeline pipeline = Channels.pipeline();

    pipeline.addLast("tracker", connectionTracker);
    pipeline.addLast("decoder", new HttpRequestDecoder());
    pipeline.addLast("encoder", new HttpResponseEncoder());
    pipeline.addLast("deflater", new HttpContentCompressor());
    pipeline.addLast("executor", executionHandler);
    pipeline.addLast("dispatcher", dispatcher);

    return pipeline;
  }
}
