package com.continuuity.internal.app.runtime.procedure;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

/**
 *
 */
public final class ProcedurePipelineFactory implements ChannelPipelineFactory {

  @Override
  public ChannelPipeline getPipeline() throws Exception {
    ChannelPipeline pipeline = Channels.pipeline();

    pipeline.addLast("decoder", new HttpRequestDecoder());
    pipeline.addLast("encoder", new HttpResponseEncoder());
    pipeline.addLast("deflater", new HttpContentCompressor());
    // TODO: Add executor
    pipeline.addLast("dispatcher", new ProcedureDispatcher());

    return pipeline;
  }
}
