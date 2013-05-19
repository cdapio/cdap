package com.continuuity.gateway.util;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class builds an http pipeline for Netty. Note that all of our Http
 * pipelines have a common pipeline, implementing the Http protocol, with
 * the main difference being their actual request handler (which has all
 * the business logic, and) which is provided by a handler factory. All
 * other features of the Http pipeline are configure via the HttpConfig.
 */

public class NettyHttpPipelineFactory implements ChannelPipelineFactory {

  private static final Logger LOG = LoggerFactory
    .getLogger(NettyHttpPipelineFactory.class);

  /**
   * creates the request handler for each new instance of the pipeline.
   */
  private NettyRequestHandlerFactory handlerFactory;
  /**
   * provides all the http protocol specific configuration.
   */
  private HttpConfig config;

  /**
   * disallow default constructor.
   */
  private NettyHttpPipelineFactory() {
  }

  /**
   * Only allowed constructor, to make sure we always have an HTTpConfig
   * and a handler factory.
   *
   * @param config         provides all the options for the http protocol
   * @param handlerFactory to create an new request handler for each new
   *                       instance of the pipeline
   */
  public NettyHttpPipelineFactory(HttpConfig config,
                                  NettyRequestHandlerFactory handlerFactory)
    throws Exception {
    this.handlerFactory = handlerFactory;
    this.config = config;
    if (this.config.isSsl()) {
      LOG.error("Attempt to create an SSL server, " +
                  "which is not implemented yet.");
      throw new UnsupportedOperationException("SSL is not yet supported");
    }
  }

  @Override // to implement the Netty PipelineFactory
  public ChannelPipeline getPipeline() throws Exception {
    // create a default (empty) pipeline
    ChannelPipeline pipeline = Channels.pipeline();

    // SSL is not yet implemented but this is where we would insert it
    if (this.config.isSsl()) {
      // SSLEngine engine = ...
      // engine.setUseClientMode(false);
      // pipeline.addLast("ssl", new SslHandler(engine));
    }

    // use the default HTTP decoder from netty
    pipeline.addLast("decoder", new HttpRequestDecoder());
    // use netty's default de-chunker
    if (this.config.isChunking()) {
      pipeline.addLast("aggregator",
                       new HttpChunkAggregator(this.config.getMaxContentSize()));
    }
    // use the default HTTP encoder from netty
    pipeline.addLast("encoder", new HttpResponseEncoder());
    // use our own request handler
    pipeline.addLast("handler", this.handlerFactory.newHandler());

    return pipeline;
  }
}
