/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.logbuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.cdap.cdap.api.logging.AppenderContext;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsConfigurer;
import io.cdap.cdap.common.service.RetryOnStartFailureService;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.logging.framework.LogPipelineLoader;
import io.cdap.cdap.logging.framework.LogPipelineSpecification;
import io.cdap.cdap.logging.logbuffer.cleaner.LogBufferCleaner;
import io.cdap.cdap.logging.logbuffer.handler.LogBufferHandler;
import io.cdap.cdap.logging.logbuffer.recover.LogBufferRecoveryService;
import io.cdap.cdap.logging.meta.CheckpointManager;
import io.cdap.cdap.logging.meta.CheckpointManagerFactory;
import io.cdap.cdap.logging.pipeline.LogProcessorPipelineContext;
import io.cdap.cdap.logging.pipeline.logbuffer.LogBufferPipelineConfig;
import io.cdap.cdap.logging.pipeline.logbuffer.LogBufferProcessorPipeline;
import io.cdap.cdap.security.URIScheme;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Log Buffer Service responsible for:
 * loading, starting and stopping log buffer pipelines
 * creating concurrent writer
 * starting log buffer cleaner service to clean up logs that have been persisted by log buffer pipelines
 * recovering logs from log buffer
 * starting netty-http service to expose endpoint to process logs.
 */
public class LogBufferService extends AbstractIdleService {
  private final DiscoveryService discoveryService;
  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final Provider<AppenderContext> contextProvider;
  private final CheckpointManagerFactory checkpointManagerFactory;
  private final List<Service> pipelines = new ArrayList<>();
  private final List<CheckpointManager<LogBufferFileOffset>> checkpointManagers = new ArrayList<>();

  private Cancellable cancellable;
  private NettyHttpService httpService;
  private LogBufferRecoveryService recoveryService;

  @Inject
  public LogBufferService(CConfiguration cConf, SConfiguration sConf, DiscoveryService discoveryService,
                          CheckpointManagerFactory checkpointManagerFactory,
                          Provider<AppenderContext> contextProvider) {
    this.cConf = cConf;
    this.sConf = sConf;
    this.contextProvider = contextProvider;
    this.checkpointManagerFactory = checkpointManagerFactory;
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    // load log pipelines
    List<LogBufferProcessorPipeline> bufferPipelines = loadLogPipelines();
    // start all the log pipelines
    validateAllFutures(Iterables.transform(pipelines, Service::start));

    // recovery service and http handler will send log events to log pipelines. In order to avoid deleting file while
    // reading them in recovery service, we will pass in an atomic boolean will be set to true by recovery service
    // when it is done recovering data. So while recovery service is running, cleanup task will be a no-op
    AtomicBoolean startCleanup = new AtomicBoolean(false);
    // start log recovery service to recover all the pending logs.
    recoveryService = new LogBufferRecoveryService(cConf, bufferPipelines, checkpointManagers, startCleanup);
    recoveryService.startAndWait();

    // create concurrent writer
    ConcurrentLogBufferWriter concurrentWriter = new ConcurrentLogBufferWriter(cConf, bufferPipelines,
                                                                               new LogBufferCleaner(cConf,
                                                                                                    checkpointManagers,
                                                                                                    startCleanup));

    // create and start http service
    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.LOG_BUFFER_SERVICE)
      .setHttpHandlers(new LogBufferHandler(concurrentWriter))
      .setExceptionHandler(new HttpExceptionHandler())
      .setHost(cConf.get(Constants.LogBuffer.LOG_BUFFER_SERVER_BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.LogBuffer.LOG_BUFFER_SERVER_BIND_PORT));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsConfigurer(cConf, sConf).enable(builder);
    }

    httpService = builder.build();
    httpService.start();
    cancellable = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.LOG_BUFFER_SERVICE, httpService)));
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      if (cancellable != null) {
        cancellable.cancel();
      }
    } finally {
      stopAllServices();
    }
  }

  /**
   * Blocks and validates all the given futures completed successfully.
   */
  private void validateAllFutures(Iterable<? extends ListenableFuture<?>> futures) throws Exception {
    // The get call shouldn't throw exception. It just block until all futures completed.
    Futures.successfulAsList(futures).get();

    // Iterates all futures to make sure all of them completed successfully
    Throwable exception = null;
    for (ListenableFuture<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        if (exception == null) {
          exception = e.getCause();
        } else {
          exception.addSuppressed(e.getCause());
        }
      }
    }
  }

  /**
   * Load log buffer pipelines.
   */
  @SuppressWarnings("unchecked")
  private List<LogBufferProcessorPipeline> loadLogPipelines() {
    Map<String, LogPipelineSpecification<AppenderContext>> specs = new LogPipelineLoader(cConf).load(contextProvider);
    int pipelineCount = specs.size();
    List<LogBufferProcessorPipeline> bufferPipelines = new ArrayList<>();
    // Create one LogBufferProcessorPipeline per spec
    for (LogPipelineSpecification<AppenderContext> pipelineSpec : specs.values()) {
      CConfiguration cConf = pipelineSpec.getConf();
      AppenderContext context = pipelineSpec.getContext();
      long bufferSize = getBufferSize(pipelineCount, cConf);
      LogBufferPipelineConfig config =
        new LogBufferPipelineConfig(bufferSize, cConf.getLong(Constants.Logging.PIPELINE_EVENT_DELAY_MS),
                                    cConf.getLong(Constants.Logging.PIPELINE_CHECKPOINT_INTERVAL_MS),
                                    cConf.getLong(Constants.LogBuffer.LOG_BUFFER_PIPELINE_BATCH_SIZE, 1000));

      CheckpointManager checkpointManager = checkpointManagerFactory.create(pipelineSpec.getCheckpointPrefix(),
                                                                            CheckpointManagerFactory.Type.LOG_BUFFER);
      LogBufferProcessorPipeline pipeline = new LogBufferProcessorPipeline(
        new LogProcessorPipelineContext(cConf, context.getName(), context,
                                        context.getMetricsContext(), context.getInstanceId()), config,
        checkpointManager, 0);
      RetryStrategy retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.log.process.");
      pipelines.add(new RetryOnStartFailureService(() -> pipeline, retryStrategy));
      bufferPipelines.add(pipeline);
      checkpointManagers.add(checkpointManager);
    }

    return bufferPipelines;
  }

  /**
   * Determines the buffer size for one pipeline.
   */
  private long getBufferSize(int numberOfPipelines, CConfiguration cConf) {
    long bufferSize = cConf.getLong(Constants.Logging.PIPELINE_BUFFER_SIZE);
    if (bufferSize > 0) {
      return bufferSize;
    }

    double bufferRatio = cConf.getDouble(Constants.Logging.PIPELINE_AUTO_BUFFER_RATIO);
    Preconditions.checkArgument(bufferRatio > 0 && bufferRatio < 1,
                                "Config %s must be between 0 and 1", Constants.Logging.PIPELINE_AUTO_BUFFER_RATIO);

    bufferSize = (long) ((Runtime.getRuntime().maxMemory() * bufferRatio) / numberOfPipelines);
    return bufferSize > 0 ? bufferSize : 1L;
  }

  private void stopAllServices() throws Exception {
    if (httpService != null) {
      httpService.stop();
    }
    if (recoveryService != null) {
      recoveryService.stopAndWait();
    }
    // Stops all pipeline
    validateAllFutures(Iterables.transform(pipelines, Service::stop));
  }
}
