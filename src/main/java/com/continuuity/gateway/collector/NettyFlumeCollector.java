/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.gateway.collector;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * This is a Flume collector built directly on Netty using Avro IPC.
 * It relies on a FlumeAdapter to do the actual ingestion of events
 * (see FlumeCollector).
 */
public class NettyFlumeCollector extends FlumeCollector {

  private static final Logger LOG = LoggerFactory
      .getLogger(NettyFlumeCollector.class);

  /**
   * the avro server
   */
  private Server server;

  @Override
  public void start() {

    LOG.debug("Starting up " + this);

    // this is all standard avro ipc. The key is to pass in Flume's avro
    // source protocol as the interface, and the FlumeAdapter as its
    // implementation.
    this.server = new NettyServer(
        new SpecificResponder(AvroSourceProtocol.class, this.flumeAdapter),
        new InetSocketAddress(this.getPort()));
    this.server.start();

    LOG.info("Collector '" + this.getName() + "' started on port " + port + ".");
  }

  @Override
  public void stop() {
    LOG.debug("Stopping " + this);
    try {
      this.server.close();
      this.server.join();
    } catch (InterruptedException e) {
      LOG.info("Received interrupt during join.");
    }
    LOG.debug("Stopped " + this);
  }
}
