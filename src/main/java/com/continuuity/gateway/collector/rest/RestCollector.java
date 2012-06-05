package com.continuuity.gateway.collector.rest;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Collector;
import com.continuuity.gateway.util.NettyHttpPipelineFactory;
import com.continuuity.gateway.util.NettyRequestHandlerFactory;
import com.continuuity.gateway.util.HttpConfig;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class RestCollector extends Collector implements NettyRequestHandlerFactory {

	private static final Logger LOG = LoggerFactory
			.getLogger(RestCollector.class);

	private static final HttpConfig defaultConfig = new HttpConfig("rest")
			.setPort(8765)
			.setPath("/stream/");

	private HttpConfig config = defaultConfig;
	private Channel serverChannel;

	public HttpConfig getHttpConfig() {
		return this.config;
	}

	@Override
	public SimpleChannelUpstreamHandler newHandler() {
		return new RestHandler(this);
	}

	@Override
	public void configure(CConfiguration configuration) throws Exception {
		super.configure(configuration);
		this.config = HttpConfig.configure(this.name, configuration, defaultConfig);
	}

	@Override
	public void start() throws Exception {
    LOG.debug("Starting up " + this);
    InetSocketAddress address = new InetSocketAddress(this.config.getPort());
		try {
			ServerBootstrap bootstrap = new ServerBootstrap(
					new NioServerSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()));
			bootstrap.setPipelineFactory(new NettyHttpPipelineFactory(this.config, this));
			this.serverChannel = bootstrap.bind(address);
		} catch (Exception e) {
			LOG.error("Failed to startup collector '" + this.getName() + "' at " + address + ".");
			throw e;
		}
		LOG.info("Collector '" + this.getName() + "' started at " + address + ".");
	}

	@Override
	public void stop() {
    LOG.debug("Stopping " + this);
    this.serverChannel.close();
    LOG.debug("Stopped " + this);
  }
}
