package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbortOnTimeoutEventHandler;
import com.continuuity.metrics.runtime.MetricsTwillRunnable;
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

    TwillSpecification.Builder.MoreRunnable serviceRunnables = TwillSpecification.Builder.with()
                                                              .setName(NAME).withRunnable();
    addMetricsService(serviceRunnables);
    TwillSpecification.Builder.RunnableSetter runnableSetter = addTransactionService(serviceRunnables);

    return runnableSetter.anyOrder()
      .withEventHandler(new AbortOnTimeoutEventHandler(noContainerTimeout))
      .build();
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
}
