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

package co.cask.cdap.data.runtime.main;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.AbortOnTimeoutEventHandler;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.logging.run.LogSaverTwillRunnable;
import co.cask.cdap.metrics.runtime.MetricsProcessorTwillRunnable;
import co.cask.cdap.metrics.runtime.MetricsTwillRunnable;
import com.google.common.base.Preconditions;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * TwillApplication wrapper for Master Services running in YARN.
 */
public class MasterTwillApplication implements TwillApplication {
  private static final Logger LOG = LoggerFactory.getLogger(MasterTwillApplication.class);
  private static final String NAME = Constants.Service.MASTER_SERVICES;

  private final CConfiguration cConf;
  private final File cConfFile;
  private final File hConfFile;

  private final Map<String, Integer> instanceCountMap;
  private final Map<String, LocalizeResource> exploreDependencies;

  public MasterTwillApplication(CConfiguration cConf, File cConfFile, File hConfFile,
                                Map<String, Integer> instanceCountMap,
                                Map<String, LocalizeResource> exploreDependencies) {
    this.cConf = cConf;
    this.cConfFile = cConfFile;
    this.hConfFile = hConfFile;
    this.instanceCountMap = instanceCountMap;
    this.exploreDependencies = exploreDependencies;
  }

  @Override
  public TwillSpecification configure() {
    // It is always present in cdap-default.xml
    final long noContainerTimeout = cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE);

    TwillSpecification.Builder.RunnableSetter runnableSetter =
      addMessaging(
        addDatasetOpExecutor(
          addLogSaverService(
            addStreamService(
              addTransactionService(
                addMetricsProcessor (
                  addMetricsService(
                    TwillSpecification.Builder.with().setName(NAME).withRunnable()
                  )
                )
              )
            )
          )
        )
      );

    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      LOG.info("Adding explore runnable.");
      runnableSetter = addExploreService(runnableSetter);
    } else {
      LOG.info("Explore module disabled - will not launch explore runnable.");
    }
    return runnableSetter
        .withPlacementPolicy()
          .add(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED, Constants.Service.STREAMS)
        .anyOrder()
        .withEventHandler(new AbortOnTimeoutEventHandler(noContainerTimeout))
        .build();
  }

  private TwillSpecification.Builder.RunnableSetter addLogSaverService(TwillSpecification.Builder.MoreRunnable
                                                                         builder) {

    int numInstances = instanceCountMap.get(Constants.Service.LOGSAVER);
    Preconditions.checkArgument(numInstances > 0, "log saver num instances should be at least 1, got %s",
                                numInstances);

    int memory = cConf.getInt(Constants.LogSaver.MEMORY_MB);
    Preconditions.checkArgument(memory > 0, "Got invalid memory value for log saver %s", memory);
    int cores = cConf.getInt(Constants.LogSaver.NUM_CORES);
    Preconditions.checkArgument(cores > 0, "Got invalid num cores value for log saver %s", cores);

    ResourceSpecification spec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(cores)
      .setMemory(memory, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(numInstances)
      .build();

    return builder.add(new LogSaverTwillRunnable(Constants.Service.LOGSAVER, "hConf.xml", "cConf.xml"), spec)
      .withLocalFiles()
      .add("hConf.xml", hConfFile.toURI())
      .add("cConf.xml", cConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addMetricsProcessor(TwillSpecification.Builder.MoreRunnable
                                                                          builder) {

    int numCores = cConf.getInt(Constants.MetricsProcessor.NUM_CORES);
    int memoryMB = cConf.getInt(Constants.MetricsProcessor.MEMORY_MB);
    int instances = instanceCountMap.get(Constants.Service.METRICS_PROCESSOR);

    ResourceSpecification metricsProcessorSpec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(numCores)
      .setMemory(memoryMB, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    return builder.add(new MetricsProcessorTwillRunnable(Constants.Service.METRICS_PROCESSOR, "cConf.xml", "hConf.xml"),
                       metricsProcessorSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addMetricsService(TwillSpecification.Builder.MoreRunnable
                                                                        builder) {
    int metricsNumCores = cConf.getInt(Constants.Metrics.NUM_CORES);
    int metricsMemoryMb = cConf.getInt(Constants.Metrics.MEMORY_MB);
    int metricsInstances = instanceCountMap.get(Constants.Service.METRICS);

    ResourceSpecification metricsSpec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(metricsNumCores)
      .setMemory(metricsMemoryMb, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(metricsInstances)
      .build();

    return builder.add(new MetricsTwillRunnable(Constants.Service.METRICS, "cConf.xml", "hConf.xml"), metricsSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();

  }

  private TwillSpecification.Builder.RunnableSetter addTransactionService(TwillSpecification.Builder.MoreRunnable
                                                                            builder) {
    int txNumCores = cConf.getInt(Constants.Transaction.Container.NUM_CORES);
    int txMemoryMb = cConf.getInt(Constants.Transaction.Container.MEMORY_MB);
    int txInstances = instanceCountMap.get(Constants.Service.TRANSACTION);

    ResourceSpecification transactionSpec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(txNumCores)
      .setMemory(txMemoryMb, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(txInstances)
      .build();

    return builder.add(new TransactionServiceTwillRunnable(Constants.Service.TRANSACTION, "cConf.xml", "hConf.xml"),
                       transactionSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addStreamService(TwillSpecification.Builder.MoreRunnable builder) {
    int instances = instanceCountMap.get(Constants.Service.STREAMS);

    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(cConf.getInt(Constants.Stream.CONTAINER_VIRTUAL_CORES))
      .setMemory(cConf.getInt(Constants.Stream.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    return builder.add(new StreamHandlerRunnable(Constants.Service.STREAMS, "cConf.xml", "hConf.xml"), resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addDatasetOpExecutor(
    TwillSpecification.Builder.MoreRunnable builder) {
    int instances = instanceCountMap.get(Constants.Service.DATASET_EXECUTOR);

    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(cConf.getInt(Constants.Dataset.Executor.CONTAINER_VIRTUAL_CORES))
      .setMemory(cConf.getInt(Constants.Dataset.Executor.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    return builder.add(
      new DatasetOpExecutorServerTwillRunnable(Constants.Service.DATASET_EXECUTOR, "cConf.xml", "hConf.xml"),
      resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addExploreService(TwillSpecification.Builder.MoreRunnable builder) {
    int instances = instanceCountMap.get(Constants.Service.EXPLORE_HTTP_USER_SERVICE);

    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(cConf.getInt(Constants.Explore.CONTAINER_VIRTUAL_CORES))
      .setMemory(cConf.getInt(Constants.Explore.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    TwillSpecification.Builder.MoreFile twillSpecs =
      builder.add(
        new ExploreCustomClassLoaderTwillRunnable(
          new ExploreServiceTwillRunnable(Constants.Service.EXPLORE_HTTP_USER_SERVICE, "cConf.xml", "hConf.xml")
            .configure()),
        resourceSpec)
        .withLocalFiles()
        .add("cConf.xml", cConfFile.toURI())
        .add("hConf.xml", hConfFile.toURI());

    for (Map.Entry<String, LocalizeResource> entry : exploreDependencies.entrySet()) {
      LocalizeResource resource = entry.getValue();
      LOG.debug("Adding {} for explore.service", resource.getURI());
      twillSpecs = twillSpecs.add(entry.getKey(), resource.getURI(), resource.isArchive());
    }

    return twillSpecs.apply();
  }

  private TwillSpecification.Builder.RunnableSetter addMessaging(TwillSpecification.Builder.MoreRunnable builder) {
    int instances = instanceCountMap.get(Constants.Service.MESSAGING_SERVICE);

    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(cConf.getInt(Constants.MessagingSystem.CONTAINER_VIRTUAL_CORES))
      .setMemory(cConf.getInt(Constants.MessagingSystem.CONTAINER_MEMORY_MB), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    return builder.add(new MessagingServiceTwillRunnable(Constants.Service.MESSAGING_SERVICE, "cConf.xml", "hConf.xml"),
                       resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();

  }
}
