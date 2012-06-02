package com.continuuity.gateway.connector.rest;

import com.continuuity.gateway.Connector;
import com.continuuity.gateway.Constants;
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
	public static String DefaultPrefix = "";
	public static String DefaultPath = "/stream/";
	public static boolean DefaultChunking = true;
	public static boolean DefaultSsl = false;

	private int port = DefaultPort;
	private String prefix = DefaultPrefix;
	private String path = DefaultPath;
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
		this.port = configuration.getInt(Constants.connectorConfigName(
				this.name, Constants.CONFIG_PORTNUMBER), DefaultPort);
		this.chunk = configuration.getBoolean(Constants.connectorConfigName(
				this.name, Constants.CONFIG_CHUNKING), true);
		this.ssl = configuration.getBoolean(Constants.connectorConfigName(
				this.name, Constants.CONFIG_SSL), false);
		this.prefix = configuration.get(Constants.connectorConfigName(
				this.name, Constants.CONFIG_PATH_PREFIX), DefaultPrefix);
		this.path = configuration.get(Constants.connectorConfigName(
				this.name, Constants.CONFIG_PATH_STREAM), DefaultPath);
		if (this.ssl) {
			LOG.warn("SSL is not implemented yet. Ignoring configuration for connector '" + this.getName() + "'.");
			this.ssl = false;
		}
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
			bootstrap.setPipelineFactory(new PipelineFactory(this));
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
