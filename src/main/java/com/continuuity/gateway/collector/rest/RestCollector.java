package com.continuuity.gateway.collector.rest;

import com.continuuity.gateway.Collector;
import com.continuuity.gateway.Constants;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class RestCollector extends Collector {

	private static final Logger LOG = LoggerFactory
			.getLogger(RestCollector.class);

	public static final int DefaultPort = 8765;
	public static String DefaultPrefix = "";
	public static String DefaultPath = "/stream/";
	public static boolean DefaultChunking = true;
	public static boolean DefaultSsl = false;

	private int port = DefaultPort;
	private String prefix = DefaultPrefix;
	private String path = DefaultPath;
	private boolean chunk = true;
	private boolean ssl = false;

	public int getPort() {
		return this.port;
	}
	public String getPrefix() {
		return this.prefix;
	}
	public String getPath() {
		return this.path;
	}
	public boolean isChunking() {
		return this.chunk;
	}
	public boolean isSsl() {
		return this.ssl;
	}

	private Channel serverChannel;

	@Override
	public void configure(Configuration configuration) throws Exception {
		super.configure(configuration);
		this.port = configuration.getInt(Constants.buildCollectorPropertyName(
				this.name, Constants.CONFIG_PORT), DefaultPort);
		this.chunk = configuration.getBoolean(Constants.buildCollectorPropertyName(
				this.name, Constants.CONFIG_CHUNKING), DefaultChunking);
		this.ssl = configuration.getBoolean(Constants.buildCollectorPropertyName(
				this.name, Constants.CONFIG_SSL), DefaultSsl);
		this.prefix = configuration.get(Constants.buildCollectorPropertyName(
				this.name, Constants.CONFIG_PATH_PREFIX), DefaultPrefix);
		this.path = configuration.get(Constants.buildCollectorPropertyName(
				this.name, Constants.CONFIG_PATH_STREAM), DefaultPath);
		if (this.ssl) {
			LOG.warn("SSL is not implemented yet. Ignoring configuration for collector '" + this.getName() + "'.");
			this.ssl = false;
		}
	}

	@Override
	public void start() throws Exception {
    LOG.debug("Starting up " + this);
    InetSocketAddress address = new InetSocketAddress(this.port);
		try {
			ServerBootstrap bootstrap = new ServerBootstrap(
					new NioServerSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()));
			bootstrap.setPipelineFactory(new PipelineFactory(this));
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
