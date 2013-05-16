/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.metrics;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.discovery.ServicePayload;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.FixedLengthFrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
final class MetricsClient extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsClient.class);

  /**
   * Connection timeout.
   */
  private static final long CONNECT_TIMEOUT = 100 * 1000L;

  /**
   * Specifies the maximum back-off time (in seconds).
   */
  private static final int BACKOFF_MAX_TIME = 30;

  /**
   * Specifies the minimum back-off time (in seconds).
   */
  private static final int BACKOFF_MIN_TIME = 1;

  /**
   * Specifies the exponent to be used for backing off.
   */
  private static final int BACKOFF_EXPONENT = 2;

  private final CConfiguration configuration;
  private final BlockingQueue<String> queue;

  private Thread runThread;
  private ClientBootstrap bootstrap;
  private ChannelGroup channelGroup;

  // TODO (ternece) : replace with DiscoverServiceClient.
  private ServiceDiscoveryClient serviceDiscovery;

  MetricsClient(CConfiguration configuration) {
    this.configuration = configuration;
    this.queue = new LinkedBlockingQueue<String>();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting MetricsClient");
    runThread = Thread.currentThread();

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("kafka-client-netty-%d")
      .setDaemon(true)
      .build();
    channelGroup = new DefaultChannelGroup();
    bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newSingleThreadExecutor(threadFactory),
                                                                      Executors.newFixedThreadPool(4, threadFactory)));
    bootstrap.setPipelineFactory(new MetricClientPipelineFactory());
    bootstrap.setOption("connectTimeoutMillis", CONNECT_TIMEOUT);

    serviceDiscovery = new ServiceDiscoveryClient(
      configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE,
                        Constants.DEFAULT_ZOOKEEPER_ENSEMBLE)
    );
    LOG.info("MetricsClient started");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping MetricsClient");
    serviceDiscovery.close();
    channelGroup.close().await();
    bootstrap.releaseExternalResources();
    LOG.info("MetricsClient stopped");
  }

  @Override
  protected void triggerShutdown() {
    runThread.interrupt();
  }

  @Override
  protected void run() throws Exception {

    final AtomicReference<Channel> writeChannelRef = new AtomicReference<Channel>();
    long nextConnectTime = 0;
    long interval = BACKOFF_MIN_TIME;

    while (isRunning()) {
      Channel writeChannel = writeChannelRef.get();
      // Try to establish connection to collection server if not yet connected/disconnected.
      if (writeChannel == null || !writeChannel.isConnected()) {
        // Calculate the sleepTime. It's for backing off reconnection attempt.
        long nanoTime = System.nanoTime();
        long sleepTime = nextConnectTime - nanoTime;
        if (sleepTime > 0) {
          TimeUnit.NANOSECONDS.sleep(sleepTime);
        }
        connect(writeChannelRef);

        // Regardless of connection result, always update the nextConnectTime and increase the interval.
        // Assumption is that after connection is established, the connection will be used at least once,
        // hence resetting the interval to BACKOFF_MIN_TIME (in the else part).
        // Otherwise, the interval will be exponential increased until it reaches BACKOFF_MAX_TIME.
        nextConnectTime = nanoTime + interval;
        interval = Math.min(BACKOFF_MAX_TIME, interval * BACKOFF_EXPONENT);

      } else {
        interval = BACKOFF_MIN_TIME;

        try {
          final String cmd = queue.take();
          Channels.write(writeChannel, cmd).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (!future.isSuccess()) {
                future.getChannel().close();
                LOG.warn("Attempted to send metric to overlord, failed due to session failures. [ {} ]",
                         cmd, future.getCause());
              }
            }
          });
        } catch (InterruptedException e) {
          // Interrupted from shutdown. Ok to continue.
          LOG.info("Metric client interrupted.");
        }
      }
    }
  }

  /**
   * Writes the metric to be sent to queue.
   *
   * @param buffer contains the command to be sent to the server.
   * @return true if successfully put on the queue else false.
   */
  public boolean write(String buffer) {
    Preconditions.checkNotNull(buffer);
    return queue.offer(buffer);
  }

  /**
   * Connects to metric collection server. If no endpoint exists, this method returns immediately
   * without modifying the channelRef. Otherwise it blocks until connection is established (or failed).
   * If connection is established successfully, the Channel object will be set to channelRef.
   */
  private void connect(final AtomicReference<Channel> channelRef) throws Exception {
    InetSocketAddress endpoint = getEndpoint();
    if (endpoint != null) {
      LOG.info("Try to connect: " + endpoint);
      final CountDownLatch latch = new CountDownLatch(1);

      bootstrap.connect(endpoint).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            Channel channel = future.getChannel();
            channelGroup.add(channel);
            channelRef.set(channel);
          }
          latch.countDown();
        }
      });
      latch.await();
      LOG.info("Connected: " + endpoint);
    } else {
      LOG.info("No endpoint to connect.");
    }
  }

  /**
   * Returns metric collection server endpoint or {@code null} if no endpoint available.
   */
  private InetSocketAddress getEndpoint() throws ServiceDiscoveryClientException {
    ServiceInstance<ServicePayload> instance =
      serviceDiscovery.getInstance(Constants.SERVICE_METRICS_COLLECTION_SERVER, new RandomStrategy<ServicePayload>());
    if (instance == null) {
      return null;
    }

    String hostname = instance.getAddress();
    int port = instance.getPort();
    LOG.info("Received service endpoint {}:{}.", hostname, port);

    return new InetSocketAddress(hostname, port);
  }

  /**
   * Pipeline factory for metric requests.
   */
  private static final class MetricClientPipelineFactory implements ChannelPipelineFactory {

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      pipeline.addLast("frameDecoder", new FixedLengthFrameDecoder(Integer.SIZE / 8));
      pipeline.addLast("requestEncoder", new MetricRequestEncoder());
      pipeline.addLast("responseDecoder", new MetricResponseDecoder());
      pipeline.addLast("responseHandler", new MetricResponseHandler());
      return pipeline;
    }
  }

  /**
   * Encoder to write metrics command to server.
   */
  private static final class MetricRequestEncoder extends OneToOneEncoder {
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (msg instanceof String) {
        String cmd = (String) msg;
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(cmd.length() + 1);
        buffer.writeBytes(Charsets.UTF_8.encode(cmd));
        buffer.writeByte('\n');
        return buffer;
      }
      return msg;
    }
  }

  /**
   * Decoder to decode server response in MetricResponse.
   */
  private static final class MetricResponseDecoder extends OneToOneDecoder {
    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
      if (msg instanceof ChannelBuffer) {
        int code = ((ChannelBuffer) msg).readInt();

        for (MetricResponse.Status status : MetricResponse.Status.values()) {
          if (code == status.getCode()) {
            return new MetricResponse(status);
          }
        }
        return new MetricResponse(MetricResponse.Status.IGNORED);
      }
      return msg;
    }
  }

  /**
   * Handler for handling MetricResponse.
   */
  private static final class MetricResponseHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      Object msg = e.getMessage();
      if (msg instanceof MetricResponse) {
        MetricResponse response = (MetricResponse) msg;
        switch (response.getStatus()) {
          case FAILED:
            LOG.warn("Failed processing metric on the overlord server. Request server logs.");
            break;
          case IGNORED:
            LOG.warn("Server ignored the data point due to capacity.");
            break;
          case INVALID:
            LOG.warn("Invalid request was sent to the server.");
            break;
          case SERVER_ERROR:
            LOG.warn("Internal server error.");
            break;
        }
      } else {
        LOG.warn("Invalid message received from the server. Message : {}", e.getMessage());
      }
    }
  }
}
