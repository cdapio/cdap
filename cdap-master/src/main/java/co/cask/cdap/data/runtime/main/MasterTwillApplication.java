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
        .withOrder().begin(Constants.Service.MESSAGING_SERVICE)
        .withEventHandler(new AbortOnTimeoutEventHandler(noContainerTimeout))
        .build();
  }

  private TwillSpecification.Builder.RunnableSetter addLogSaverService(TwillSpecification.Builder.MoreRunnable
                                                                         builder) {

    ResourceSpecification resourceSpec = createResourceSpecification(Constants.LogSaver.NUM_CORES,
                                                                     Constants.LogSaver.MEMORY_MB,
                                                                     Constants.Service.LOGSAVER);

    return builder.add(new LogSaverTwillRunnable(Constants.Service.LOGSAVER, "cConf.xml", "hConf.xml"), resourceSpec)
      .withLocalFiles()
      .add("hConf.xml", hConfFile.toURI())
      .add("cConf.xml", cConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addMetricsProcessor(
    TwillSpecification.Builder.MoreRunnable builder) {

    ResourceSpecification resourceSpec = createResourceSpecification(Constants.MetricsProcessor.NUM_CORES,
                                                                     Constants.MetricsProcessor.MEMORY_MB,
                                                                     Constants.Service.METRICS_PROCESSOR);

    return builder.add(new MetricsProcessorTwillRunnable(Constants.Service.METRICS_PROCESSOR, "cConf.xml", "hConf.xml"),
                       resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addMetricsService(TwillSpecification.Builder.MoreRunnable
                                                                        builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Metrics.NUM_CORES,
                                                                     Constants.Metrics.MEMORY_MB,
                                                                     Constants.Service.METRICS);

    return builder.add(new MetricsTwillRunnable(Constants.Service.METRICS, "cConf.xml", "hConf.xml"), resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();

  }

  private TwillSpecification.Builder.RunnableSetter addTransactionService(
    TwillSpecification.Builder.MoreRunnable builder) {

    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Transaction.Container.NUM_CORES,
                                                                     Constants.Transaction.Container.MEMORY_MB,
                                                                     Constants.Service.TRANSACTION);

    return builder.add(new TransactionServiceTwillRunnable(Constants.Service.TRANSACTION, "cConf.xml", "hConf.xml"),
                       resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addStreamService(TwillSpecification.Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Stream.CONTAINER_VIRTUAL_CORES,
                                                                     Constants.Stream.CONTAINER_MEMORY_MB,
                                                                     Constants.Service.STREAMS);

    return builder.add(new StreamHandlerRunnable(Constants.Service.STREAMS, "cConf.xml", "hConf.xml"), resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addDatasetOpExecutor(
    TwillSpecification.Builder.MoreRunnable builder) {

    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Dataset.Executor.CONTAINER_VIRTUAL_CORES,
                                                                     Constants.Dataset.Executor.CONTAINER_MEMORY_MB,
                                                                     Constants.Service.DATASET_EXECUTOR);
    return builder.add(
      new DatasetOpExecutorServerTwillRunnable(Constants.Service.DATASET_EXECUTOR, "cConf.xml", "hConf.xml"),
      resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addExploreService(TwillSpecification.Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.Explore.CONTAINER_VIRTUAL_CORES,
                                                                     Constants.Explore.CONTAINER_MEMORY_MB,
                                                                     Constants.Service.EXPLORE_HTTP_USER_SERVICE);
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
    ResourceSpecification resourceSpec = createResourceSpecification(Constants.MessagingSystem.CONTAINER_VIRTUAL_CORES,
                                                                     Constants.MessagingSystem.CONTAINER_MEMORY_MB,
                                                                     Constants.Service.MESSAGING_SERVICE);

    return builder.add(new MessagingServiceTwillRunnable(Constants.Service.MESSAGING_SERVICE, "cConf.xml", "hConf.xml"),
                       resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();

  }

  private ResourceSpecification createResourceSpecification(String vCoresKey, String memoryKey, String instancesKey) {
    int vCores = cConf.getInt(vCoresKey);
    int memory = cConf.getInt(memoryKey);
    int instances = instanceCountMap.get(instancesKey);

    Preconditions.checkArgument(vCores > 0, "Number of virtual cores must be > 0 for property %s", vCoresKey);
    Preconditions.checkArgument(memory > 0, "Memory size must be > 0 for property %s", memoryKey);
    Preconditions.checkArgument(instances > 0, "Number of instances must be > 0 for property %s", instancesKey);

    return ResourceSpecification.Builder.with()
      .setVirtualCores(vCores)
      .setMemory(memory, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();
  }
}
