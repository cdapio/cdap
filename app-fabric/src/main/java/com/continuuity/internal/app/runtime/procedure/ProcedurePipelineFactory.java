package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.common.metrics.MetricsCollector;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
final class ProcedurePipelineFactory implements ChannelPipelineFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedurePipelineFactory.class);
  private static final int MAX_INPUT_SIZE = 8192;

  private final ExecutionHandler executionHandler;
  private final ProcedureDispatcher dispatcher;
  private final ChannelUpstreamHandler connectionTracker;

  ProcedurePipelineFactory(ExecutionHandler executionHandler,
                           HandlerMethodFactory handlerMethodFactory,
                           MetricsCollector metrics,
                           final ChannelGroup channelGroup) {
    this.executionHandler = executionHandler;
    this.dispatcher = new ProcedureDispatcher(handlerMethodFactory, metrics);
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
    pipeline.addLast("aggregator", new HttpChunkAggregator(MAX_INPUT_SIZE));
    pipeline.addLast("exception", new ExceptionHandler());
    pipeline.addLast("executor", executionHandler);
    pipeline.addLast("dispatcher", dispatcher);

    return pipeline;
  }

  /**
   * This class responsible for sending out error response when there is upstream exception
   * after http message being decoded (e.g. reject execution). Later upstream
   * handlers can have their own exceptionCaught to by pass this handling.
   */
  private static final class ExceptionHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      if (ctx.getChannel().isConnected()) {
        LOG.error("Got exception: ", e.getCause());

        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
        response.setContent(ChannelBuffers.EMPTY_BUFFER);
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
      } else {
        ctx.getChannel().close();
      }
    }
  }
}
