package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbortOnTimeoutEventHandler;
import com.continuuity.logging.run.LogSaverTwillRunnable;
import com.continuuity.metrics.runtime.MetricsProcessorTwillRunnable;
import com.continuuity.metrics.runtime.MetricsTwillRunnable;
import com.google.common.base.Preconditions;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.io.File;

/**
 * TwillApplication wrapper for Reactor YARN Services.
 */
public class ReactorTwillApplication implements TwillApplication {
  private static final String NAME = "reactor.services";

  private final CConfiguration cConf;
  private final File cConfFile;

  private final File hConfFile;

  public ReactorTwillApplication(CConfiguration cConf, File cConfFile, File hConfFile) {
    this.cConf = cConf;
    this.cConfFile = cConfFile;
    this.hConfFile = hConfFile;
  }

  @Override
  public TwillSpecification configure() {
    // It is always present in continuuity-default.xml
    final long noContainerTimeout = cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE);

    return
      addLogSaverService(
       addStreamService(
         addTransactionService(
           addMetricsProcessor (
             addMetricsService(
              TwillSpecification.Builder.with().setName(NAME).withRunnable())))))
        .anyOrder()
        .withEventHandler(new AbortOnTimeoutEventHandler(noContainerTimeout))
        .build();
  }

  private TwillSpecification.Builder.RunnableSetter addLogSaverService(TwillSpecification.Builder.MoreRunnable
                                                                         builder) {

    int numInstances = cConf.getInt(Constants.LogSaver.NUM_INSTANCES, 1);
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

    return builder.add(new LogSaverTwillRunnable("saver", "hConf.xml", "cConf.xml"), spec)
      .withLocalFiles()
      .add("hConf.xml", hConfFile.toURI())
      .add("cConf.xml", cConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addMetricsProcessor(TwillSpecification.Builder.MoreRunnable
                                                                          builder) {

    int numCores = cConf.getInt(Constants.MetricsProcessor.NUM_CORES, 1);
    int memoryMB = cConf.getInt(Constants.MetricsProcessor.MEMORY_MB, 512);
    int instances = cConf.getInt(Constants.MetricsProcessor.NUM_INSTANCES, 2);

    ResourceSpecification metricsProcessorSpec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(numCores)
      .setMemory(memoryMB, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    return builder.add(new MetricsProcessorTwillRunnable("metrics.processor", "cConf.xml", "hConf.xml"),
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
    int metricsInstances = cConf.getInt(Constants.Metrics.NUM_INSTANCES, 1);

    ResourceSpecification metricsSpec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(metricsNumCores)
      .setMemory(metricsMemoryMb, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(metricsInstances)
      .build();

    return builder.add(new MetricsTwillRunnable("metrics", "cConf.xml", "hConf.xml"), metricsSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();

  }

  private TwillSpecification.Builder.RunnableSetter addTransactionService(TwillSpecification.Builder.MoreRunnable
                                                                            builder) {
    int txNumCores = cConf.getInt(Constants.Transaction.Container.NUM_CORES, 2);
    int txMemoryMb = cConf.getInt(Constants.Transaction.Container.MEMORY_MB, 2048);
    int txInstances = cConf.getInt(Constants.Transaction.Container.NUM_INSTANCES, 1);

    ResourceSpecification transactionSpec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(txNumCores)
      .setMemory(txMemoryMb, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(txInstances)
      .build();

    return builder.add(new TransactionServiceTwillRunnable("txservice", "cConf.xml", "hConf.xml"), transactionSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }

  private TwillSpecification.Builder.RunnableSetter addStreamService(TwillSpecification.Builder.MoreRunnable builder) {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(cConf.getInt(Constants.Stream.CONTAINER_VIRTUAL_CORES, 1))
      .setMemory(cConf.getInt(Constants.Stream.CONTAINER_MEMORY_MB, 512), ResourceSpecification.SizeUnit.MEGA)
      .setInstances(cConf.getInt(Constants.Stream.CONTAINER_INSTANCES, 2))
      .build();

    return builder.add(new StreamHandlerRunnable("stream", "cConf.xml", "hConf.xml"), resourceSpec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply();
  }
}
