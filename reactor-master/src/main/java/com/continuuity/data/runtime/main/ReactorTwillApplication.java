package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbortOnTimeoutEventHandler;
import com.continuuity.explore.service.ExploreServiceUtils;
import com.continuuity.logging.run.LogSaverTwillRunnable;
import com.continuuity.metrics.runtime.MetricsProcessorTwillRunnable;
import com.continuuity.metrics.runtime.MetricsTwillRunnable;
import com.google.common.base.Preconditions;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * TwillApplication wrapper for Reactor YARN Services.
 */
public class ReactorTwillApplication implements TwillApplication {
  private static final Logger LOG = LoggerFactory.getLogger(ReactorServiceMain.class);
  private static final String NAME = Constants.Service.REACTOR_SERVICES;

  private final CConfiguration cConf;
  private final File cConfFile;
  private final File hConfFile;

  private final boolean runHiveService;
  private final Map<String, Integer> instanceCountMap;

  public ReactorTwillApplication(CConfiguration cConf, File cConfFile, File hConfFile, boolean runHiveService,
                                 Map<String, Integer> instanceCountMap) {
    this.cConf = cConf;
    this.cConfFile = cConfFile;
    this.hConfFile = hConfFile;
    this.runHiveService = runHiveService;
    this.instanceCountMap = instanceCountMap;
  }

  @Override
  public TwillSpecification configure() {
    // It is always present in continuuity-default.xml
    final long noContainerTimeout = cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE);

    TwillSpecification.Builder.RunnableSetter runnableSetter =
        addDatasetOpExecutor(
            addLogSaverService(
                addStreamService(
                    addTransactionService(
                        addMetricsProcessor (
                            addMetricsService(
                                TwillSpecification.Builder.with().setName(NAME).withRunnable()))))));

    if (runHiveService) {
      LOG.info("Adding explore runnable.");
      runnableSetter = addExploreService(runnableSetter);
    } else {
      LOG.info("Explore module disabled - will not launch explore runnable.");
    }
    return runnableSetter
        .anyOrder()
        .withEventHandler(new AbortOnTimeoutEventHandler(noContainerTimeout))
        .build();
  }

  private TwillSpecification.Builder.RunnableSetter addLogSaverService(TwillSpecification.Builder.MoreRunnable
                                                                         builder) {

    int numInstances = instanceCountMap.get(Constants.Service.LOGSAVER);
    Preconditions.checkArgument(numInstances > 0, "log saver num instances should be at least 1, got %s",
                                numInstances);

    int memory = cConf.getInt(Constants.LogSaver.MEMORY_MB, 1024);
    Preconditions.checkArgument(memory > 0, "Got invalid memory value for log saver %s", memory);

    ResourceSpecification spec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(2)
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

    int numCores = cConf.getInt(Constants.MetricsProcessor.NUM_CORES, 1);
    int memoryMB = cConf.getInt(Constants.MetricsProcessor.MEMORY_MB, 512);
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
    int metricsNumCores = cConf.getInt(Constants.Metrics.NUM_CORES, 2);
    int metricsMemoryMb = cConf.getInt(Constants.Metrics.MEMORY_MB, 2048);
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
    int txNumCores = cConf.getInt(Constants.Transaction.Container.NUM_CORES, 2);
    int txMemoryMb = cConf.getInt(Constants.Transaction.Container.MEMORY_MB, 2048);
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
      .setVirtualCores(cConf.getInt(Constants.Stream.CONTAINER_VIRTUAL_CORES, 1))
      .setMemory(cConf.getInt(Constants.Stream.CONTAINER_MEMORY_MB, 512), ResourceSpecification.SizeUnit.MEGA)
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
      .setVirtualCores(cConf.getInt(Constants.Dataset.Executor.CONTAINER_VIRTUAL_CORES, 1))
      .setMemory(cConf.getInt(Constants.Dataset.Executor.CONTAINER_MEMORY_MB, 512), ResourceSpecification.SizeUnit.MEGA)
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
      .setVirtualCores(cConf.getInt(Constants.Explore.CONTAINER_VIRTUAL_CORES, 1))
      .setMemory(cConf.getInt(Constants.Explore.CONTAINER_MEMORY_MB, 512), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    TwillSpecification.Builder.MoreFile twillSpecs =
      builder.add(new ExploreServiceTwillRunnable(
        Constants.Service.EXPLORE_HTTP_USER_SERVICE, "cConf.xml", "hConf.xml"), resourceSpec)
        .withLocalFiles()
        .add("cConf.xml", cConfFile.toURI())
        .add("hConf.xml", hConfFile.toURI());

    try {
      // Ship jars needed by Hive to the container
      Set<File> jars = ExploreServiceUtils.traceExploreDependencies();
      for (File jarFile : jars) {
        twillSpecs = twillSpecs.add(jarFile.getName(), jarFile);
      }
    } catch (IOException e) {
      throw new RuntimeException("Unable to trace Explore dependencies", e);
    }

    return twillSpecs.apply();
  }
}
