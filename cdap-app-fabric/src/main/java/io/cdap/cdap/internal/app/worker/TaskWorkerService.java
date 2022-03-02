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
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.service.worker.RunnableTask;
import io.cdap.cdap.app.guice.ImpersonatedTwillRunnerService;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactManagerFactory;
import io.cdap.cdap.internal.app.runtime.schedule.TimeSchedulerService;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.scheduler.CoreSchedulerService;
import io.cdap.cdap.security.auth.KeyManager;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import org.apache.twill.api.TwillRunnerService;
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
  private final KeyManager keyManager;
  private final ProvisioningService provisioningService;
  private final CoreSchedulerService coreSchedulerService;
  private final TimeSchedulerService timeSchedulerService;
  private final ProgramRuntimeService programRuntimeService;
  private final TwillRunnerService twillRunnerService;

  @Inject
  TaskWorkerService(CConfiguration cConf,
                    SConfiguration sConf,
                    DiscoveryService discoveryService,
                    KeyManager keyManager,
                    ProvisioningService provisioningService,
                    MetricsCollectionService metricsCollectionService,
                    CoreSchedulerService coreSchedulerService,
                    TimeSchedulerService timeSchedulerService,
                    ProgramRuntimeService programRuntimeService,
                    TwillRunnerService twillRunnerService) {
    this.discoveryService = discoveryService;
    this.keyManager = keyManager;
    this.provisioningService = provisioningService;
    this.coreSchedulerService = coreSchedulerService;
    this.timeSchedulerService = timeSchedulerService;
    this.programRuntimeService = programRuntimeService;
    this.twillRunnerService = twillRunnerService;
    LOG.debug("TwillRunnerService in TaskWorkerService: {}", twillRunnerService);
    if (twillRunnerService instanceof ImpersonatedTwillRunnerService) {
      LOG.debug("TwillRunnerService in TaskWorkerService internal: {}",
          ((ImpersonatedTwillRunnerService) twillRunnerService).delegate);
    }
    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.TASK_WORKER)
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
      .setHttpHandlers(new TaskWorkerHttpHandlerInternal(cConf, sConf, this::stopService,
          metricsCollectionService, keyManager, provisioningService, twillRunnerService));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting TaskWorkerService");
    keyManager.startAndWait();
    provisioningService.startAndWait();
    coreSchedulerService.startAndWait();
    timeSchedulerService.startAndWait();
    programRuntimeService.startAndWait();
    twillRunnerService.start();
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
    /** TODO: Expand this logic such that
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
