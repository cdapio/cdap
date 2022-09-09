/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Launches an HTTP server for receiving and handling {@link RunnableTask}
 */
public class TaskWorkerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskWorkerService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;

  @Inject
  TaskWorkerService(CConfiguration cConf,
                    SConfiguration sConf,
                    DiscoveryService discoveryService,
                    MetricsCollectionService metricsCollectionService,
                    CommonNettyHttpServiceFactory commonNettyHttpServiceFactory) {
    this.discoveryService = discoveryService;

    NettyHttpService.Builder builder = commonNettyHttpServiceFactory.builder(Constants.Service.TASK_WORKER)
      .setHost(cConf.get(Constants.TaskWorker.ADDRESS))
      .setPort(cConf.getInt(Constants.TaskWorker.PORT))
      .setExecThreadPoolSize(cConf.getInt(Constants.TaskWorker.EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.TaskWorker.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.TaskWorker.WORKER_THREADS))
      .setChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
        }
      })
      .setHttpHandlers(new TaskWorkerHttpHandlerInternal(cConf, this::stopService, metricsCollectionService));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting TaskWorkerService");
    httpService.start();
    bindAddress = httpService.getBindAddress();
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.TASK_WORKER, httpService)));
    LOG.debug("Starting TaskWorkerService has completed");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down TaskWorkerService");
    httpService.stop(1, 2, TimeUnit.SECONDS);
    cancelDiscovery.cancel();
    LOG.debug("Shutting down TaskWorkerService has completed");
  }

  private void stopService(String className) {
    /*
     * TODO: Expand this logic such that
     * based on number of requests per particular class,
     * the service gets stopped.
     */
    stop();
  }

  @VisibleForTesting
  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
