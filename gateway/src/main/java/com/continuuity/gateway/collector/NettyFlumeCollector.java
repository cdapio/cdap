/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * This is a Flume collector built directly on Netty using Avro IPC.
 * It relies on a FlumeAdapter to do the actual ingestion of events
 * (see FlumeCollector).
 */
public class NettyFlumeCollector extends FlumeCollector {

  private static final Logger LOG = LoggerFactory
    .getLogger(NettyFlumeCollector.class);

  /**
   * the avro server.
   */
  private Server server;

  /**
   * the max number of netty worker threads for the connector.
   */
  private int threads;

  @Override
  public void configure(CConfiguration configuration) throws Exception {
    super.configure(configuration);
    // the only additional option we need is number of netty threads
    this.threads = configuration.getInt(this.getName() + ".threads", Constants.Gateway.DEFAULT_WORKER_THREADS);
  }

  @Override
  public void start() {

    LOG.info("Starting up " + this);

    // this is all standard avro ipc. The key is to pass in Flume's avro
    // source protocol as the interface, and the FlumeAdapter as its
    // implementation.
    this.server = new NettyServer(
      new SpecificResponder(AvroSourceProtocol.class, this.flumeAdapter),
      new InetSocketAddress(this.getPort()),
      // in order to control the number of netty worker threads, we
      // must create and pass in the server channel factory explicitly
      new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool(),
        this.threads));
    this.server.start();

    LOG.info("Collector '" + this.getName() +
               "' started on port " + port + " with " + this.threads + " threads.");
  }

  @Override
  public void stop() {
    LOG.info("Stopping " + this);
    try {
      this.server.close();
      this.server.join();
    } catch (InterruptedException e) {
      LOG.info("Received interrupt during join.");
    }
    LOG.info("Stopped " + this);
  }
}
