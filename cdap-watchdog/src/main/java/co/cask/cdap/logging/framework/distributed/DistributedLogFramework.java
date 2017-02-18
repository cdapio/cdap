/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework.distributed;

import co.cask.cdap.api.logging.AppenderContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.resource.ResourceBalancerService;
import co.cask.cdap.common.service.RetryOnStartFailureService;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.logging.framework.LogPipelineLoader;
import co.cask.cdap.logging.framework.LogPipelineSpecification;
import co.cask.cdap.logging.meta.CheckpointManagerFactory;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.kafka.KafkaLogProcessorPipeline;
import co.cask.cdap.logging.pipeline.kafka.KafkaPipelineConfig;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.zookeeper.ZKClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * A {@link ResourceBalancerService} for log processing in distributed mode
 */
public class DistributedLogFramework extends ResourceBalancerService {

  private static final String SERVICE_NAME = "log.framework";

  private final CConfiguration cConf;
  private final Provider<AppenderContext> contextProvider;
  private final CheckpointManagerFactory checkpointManagerFactory;
  private final BrokerService brokerService;

  @Inject
  DistributedLogFramework(CConfiguration cConf,
                          ZKClient zkClient,
                          DiscoveryService discoveryService,
                          DiscoveryServiceClient discoveryServiceClient,
                          Provider<AppenderContext> contextProvider,
                          CheckpointManagerFactory checkpointManagerFactory,
                          BrokerService brokerService) {
    super(SERVICE_NAME, cConf.getInt(Constants.Logging.NUM_PARTITIONS),
          zkClient, discoveryService, discoveryServiceClient);
    this.cConf = cConf;
    this.contextProvider = contextProvider;
    this.checkpointManagerFactory = checkpointManagerFactory;
    this.brokerService = brokerService;
  }

  @Override
  protected Service createService(Set<Integer> partitions) {
    Map<String, LogPipelineSpecification<AppenderContext>> specs = new LogPipelineLoader(cConf).load(contextProvider);
    int pipelineCount = specs.size();

    // Create one KafkaLogProcessorPipeline per spec
    final List<Service> pipelines = new ArrayList<>();
    for (final LogPipelineSpecification<AppenderContext> pipelineSpec : specs.values()) {
      final CConfiguration cConf = pipelineSpec.getConf();
      final AppenderContext context = pipelineSpec.getContext();

      long bufferSize = getBufferSize(pipelineCount, cConf, partitions.size());
      final String topic = cConf.get(Constants.Logging.KAFKA_TOPIC);

      final KafkaPipelineConfig config = new KafkaPipelineConfig(
        topic, partitions, bufferSize,
        cConf.getLong(Constants.Logging.PIPELINE_EVENT_DELAY_MS),
        cConf.getInt(Constants.Logging.PIPELINE_KAFKA_FETCH_SIZE),
        cConf.getLong(Constants.Logging.PIPELINE_CHECKPOINT_INTERVAL_MS)
      );

      RetryStrategy retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.log.process.");
      pipelines.add(new RetryOnStartFailureService(new Supplier<Service>() {
        @Override
        public Service get() {
          return new KafkaLogProcessorPipeline(
            new LogProcessorPipelineContext(cConf, context.getName(), context),
            checkpointManagerFactory.create(topic, pipelineSpec.getCheckpointPrefix()), brokerService, config);
        }
      }, retryStrategy));
    }

    // Returns a Service that start/stop all pipelines.
    return new AbstractIdleService() {
      @Override
      protected void startUp() throws Exception {
        // Starts all pipeline
        validateAllFutures(Iterables.transform(pipelines, new Function<Service, ListenableFuture<State>>() {
          @Override
          public ListenableFuture<State> apply(Service service) {
            return service.start();
          }
        }));
      }

      @Override
      protected void shutDown() throws Exception {
        // Stops all pipeline
        validateAllFutures(Iterables.transform(pipelines, new Function<Service, ListenableFuture<State>>() {
          @Override
          public ListenableFuture<State> apply(Service service) {
            return service.stop();
          }
        }));
      }
    };
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

    // Throw exception if any of the future failed.
    if (exception != null) {
      if (exception instanceof Exception) {
        throw (Exception) exception;
      }
      throw new RuntimeException(exception);
    }
  }

  /**
   * Determines the buffer size for one pipeline.
   */
  private long getBufferSize(int numberOfPipelines, CConfiguration cConf, int partitions) {
    long bufferSize = cConf.getLong(Constants.Logging.PIPELINE_BUFFER_SIZE);
    if (bufferSize > 0) {
      return bufferSize;
    }

    double bufferRatio = cConf.getDouble(Constants.Logging.PIPELINE_AUTO_BUFFER_RATIO);
    Preconditions.checkArgument(bufferRatio > 0 && bufferRatio < 1,
                                "Config %s must be between 0 and 1", Constants.Logging.PIPELINE_AUTO_BUFFER_RATIO);

    int kafkaFetchSize = cConf.getInt(Constants.Logging.PIPELINE_KAFKA_FETCH_SIZE) * partitions;

    // Try to derive it from the total memory size, the number of pipelines and the kafka fetch size
    bufferSize = (long) ((Runtime.getRuntime().maxMemory() * bufferRatio - kafkaFetchSize) / numberOfPipelines);
    // The size has to be > 0 for it to make any progress. This is just to safe guard
    return bufferSize > 0 ? bufferSize : 1L;
  }
}
