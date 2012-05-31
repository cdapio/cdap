/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Created by: andreas, having fun since April 2012
 */
public class NettyFlumeConnector extends FlumeConnector {

	private static final Logger LOG = LoggerFactory
			.getLogger(NettyFlumeConnector.class);

	private Server server; // the netty server

	public void configure(Configuration configuration) {	}

	public void start() {
		LOG.info("Starting: ", this);

		this.server = new NettyServer(
				new SpecificResponder(AvroSourceProtocol.class, this.flumeAdapter),
				new InetSocketAddress(this.host, this.port));
		this.consumer.start();
		this.server.start();

		LOG.debug("Started successfully", this);
	}

	public void stop() {
		LOG.info("Stopping: ", this);
		try {
			this.server.close();
			this.server.join();
		} catch (InterruptedException e) {
			LOG.info("Received interrupt during join.");
		}
		this.consumer.stop();
		LOG.debug("Stopped. ", this);
	}

	public String toString() {
		return this.getClass().getName() + " at " + this.getAddress()
				+ "(" + this.consumer.getClass().getName() + ")";
	}
}
