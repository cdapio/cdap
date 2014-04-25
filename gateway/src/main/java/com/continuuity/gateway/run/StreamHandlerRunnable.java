/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.gateway.run;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data.stream.StreamHandler;
import com.continuuity.data.stream.TimePartitionedStreamFileWriter;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * TODO: Temporarily piggy back on Gateway to start StreamHandler HTTP service.
 */
public class StreamHandlerRunnable extends AbstractTwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamHandlerRunnable.class);

  private static final String CCONF_NAME = "cConf";
  private static final String HCONF_NAME = "hConf";

  private ZKClientService zkClientService;
  private Cancellable serviceCancellable;
  private NettyHttpService httpService;
  private CountDownLatch stopLatch;

  public StreamHandlerRunnable(String cConfName, String hConfName) {
    super(ImmutableMap.of(CCONF_NAME, cConfName, HCONF_NAME, hConfName));
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);

    try {
      Map<String, String> configs = context.getSpecification().getConfigs();

      CConfiguration cConf = new CConfiguration();
      cConf.clear();
      cConf.addResource(new File(configs.get(CCONF_NAME)).toURI().toURL());

      Configuration hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get(HCONF_NAME)).toURI().toURL());

      Injector injector = Guice.createInjector(new ConfigModule(cConf, hConf),
                                               new ZKClientModule(),
                                               new DiscoveryRuntimeModule().getDistributedModules(),
                                               new LocationRuntimeModule().getDistributedModules(),
                                               new DataFabricModules(cConf, hConf).getDistributedModules());

      zkClientService = injector.getInstance(ZKClientService.class);
      LOG.info("Starting ZKClient");
      zkClientService.startAndWait();
      LOG.info("ZKClient started");

      DiscoveryService discoveryService = injector.getInstance(DiscoveryService.class);

      StreamAdmin streamAdmin = injector.getInstance(StreamAdmin.class);

      String filePrefix = "file." + context.getInstanceId();
      StreamFileWriterFactory writerFactory = new FileWriterFactory(streamAdmin, filePrefix);
      int workerThreads = Runtime.getRuntime().availableProcessors() * 2;

      httpService = NettyHttpService.builder()
        .addHttpHandlers(ImmutableList.of(new StreamHandler(streamAdmin, writerFactory, workerThreads)))
        .setHost(InetAddress.getLocalHost().getHostName())
        .setWorkerThreadPoolSize(workerThreads)
        .setExecThreadPoolSize(0)
        .setConnectionBacklog(20000)
        .build();
      stopLatch = new CountDownLatch(1);
      httpService.addListener(new ServiceListenerAdapter() {
        @Override
        public void terminated(Service.State from) {
          stopLatch.countDown();
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
          stopLatch.countDown();
        }
      }, Threads.SAME_THREAD_EXECUTOR);
      httpService.startAndWait();

      LOG.info("Stream handler service started at {}", httpService.getBindAddress());

      serviceCancellable = discoveryService.register(new Discoverable() {
        @Override
        public String getName() {
          return Constants.Service.STREAM_HANDLER;
        }

        @Override
        public InetSocketAddress getSocketAddress() {
          return httpService.getBindAddress();
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void stop() {
    httpService.stopAndWait();
  }

  @Override
  public void run() {
    try {
      stopLatch.await();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      serviceCancellable.cancel();
      zkClientService.stopAndWait();
    }
  }

  private static final class FileWriterFactory implements StreamFileWriterFactory {

    private final StreamAdmin streamAdmin;
    private final String filePrefix;

    private FileWriterFactory(StreamAdmin streamAdmin, String filePrefix) {
      this.streamAdmin = streamAdmin;
      this.filePrefix = filePrefix;
    }

    @Override
    public FileWriter<StreamEvent> create(String streamName) throws IOException {
      try {
        StreamConfig config = streamAdmin.getConfig(streamName);
        return new TimePartitionedStreamFileWriter(config, filePrefix);

      } catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new IOException(e);
      }
    }
  }
}
