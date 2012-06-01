package com.continuuity.gateway.connector.rest;

import com.continuuity.gateway.Connector;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class RestConnector extends Connector {

	private static final Logger LOG = LoggerFactory
			.getLogger(RestConnector.class);

	public static final int DefaultPort = 8765;
	private int port = DefaultPort;
	private boolean chunk = true;
	private boolean ssl = false;

	public void setPort(int port) {
		this.port = port;
	}
	public void setChunking(boolean doChunk) {
		this.chunk = doChunk;
	}
	public void setSsl(boolean doSsl) {
		this.ssl = doSsl;
	}

	private Channel serverChannel;

	@Override
	public void configure(Configuration configuration) {
		// @todo read ssl, chunk, port, path prefix from configuration
	}

	@Override
	public void start() throws Exception {
		LOG.info("Starting up.");
		InetSocketAddress address = new InetSocketAddress(this.port);
		try {
			ServerBootstrap bootstrap = new ServerBootstrap(
					new NioServerSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()));
			bootstrap.setPipelineFactory(new PipelineFactory(this, this.ssl, this.chunk));
			this.serverChannel = bootstrap.bind(address);
		} catch (Exception e) {
			LOG.error("Failed to startup connector '" + this.getName() + "' at " + address + ".");
			throw e;
		}
		LOG.info("Connector '" + this.getName() + "' started at " + address + ".");
	}

	@Override
	public void stop() {
		LOG.info("Shutting down.");
		this.serverChannel.close();
	}
}
