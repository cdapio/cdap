/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.collector;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.twill.common.Threads;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * This is a Flume collector built directly on Netty using Avro IPC.
 * It relies on a FlumeAdapter to do the actual ingestion of events.
 */
public class NettyFlumeCollector extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(NettyFlumeCollector.class);

  private final int threads;
  private final int port;
  private final FlumeAdapter flumeAdapter;

  /**
   * the avro server.
   */
  private Server server;

  @Inject
  public NettyFlumeCollector(CConfiguration cConf, FlumeAdapter flumeAdapter) {
    this.threads = cConf.getInt(Constants.Gateway.STREAM_FLUME_THREADS,
                                Constants.Gateway.DEFAULT_STREAM_FLUME_THREADS);
    this.port = cConf.getInt(Constants.Gateway.STREAM_FLUME_PORT, Constants.Gateway.DEFAULT_STREAM_FLUME_PORT);
    this.flumeAdapter = flumeAdapter;
  }

  public int getPort() {
    return server.getPort();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting NettyFlumeCollector...");

    flumeAdapter.startAndWait();

    // this is all standard avro ipc. The key is to pass in Flume's avro
    // source protocol as the interface, and the FlumeAdapter as its
    // implementation.
    this.server = new NettyServer(
      new SpecificResponder(AvroSourceProtocol.Callback.class, flumeAdapter),
      new InetSocketAddress(port),
      // in order to control the number of netty worker threads, we
      // must create and pass in the server channel factory explicitly
      new NioServerSocketChannelFactory(
        Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("flume-stream-boss")),
        Executors.newFixedThreadPool(threads, Threads.createDaemonThreadFactory("flume-stream-worker"))));
    server.start();

    LOG.info("NettyFlumeCollector started on port {} with {} threads", server.getPort(), threads);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping NettyFlumeCollector...");
    try {
      this.server.close();
      this.server.join();
    } catch (InterruptedException e) {
      LOG.info("Received interrupt during join.");
    }

    flumeAdapter.stopAndWait();
    LOG.info("Stopped NettyFlumeCollector");
  }
}
