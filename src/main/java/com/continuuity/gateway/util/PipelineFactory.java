package com.continuuity.gateway.util;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineFactory implements ChannelPipelineFactory {

	private static final Logger LOG = LoggerFactory
			.getLogger(PipelineFactory.class);

	private boolean ssl = false, chunk = false;
	private HandlerFactory handlerFactory;
	private HttpConfig config;

	/** disallow default constructor */
	private PipelineFactory() { }

	/** constructor requires settings whether to use ssl and/or chunking */
	public PipelineFactory(HttpConfig config, HandlerFactory handlerFactory) throws Exception {
		this.handlerFactory = handlerFactory;
		this.config = config;
		if (this.config.isSsl()) {
			LOG.error("Attempt to create an SSL server, which is not implemented yet.");
			throw new UnsupportedOperationException("SSL is not yet supported");
		}
	}

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
			pipeline.addLast("aggregator", new HttpChunkAggregator(this.config.getMaxContentSize()));
		}
		// use the default HTTP encoder from netty
		pipeline.addLast("encoder", new HttpResponseEncoder());
		// use our own request handler
		pipeline.addLast("handler", this.handlerFactory.newHandler());

		return pipeline;
	}
}