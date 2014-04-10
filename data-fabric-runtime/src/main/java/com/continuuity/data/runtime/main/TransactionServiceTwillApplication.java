package com.continuuity.data.runtime.main;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.twill.AbortOnTimeoutEventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.io.File;

/**
 * TwillApplication wrapper for Transaction Service.
 */
@SuppressWarnings("UnusedDeclaration")
public class TransactionServiceTwillApplication implements TwillApplication {
  private static final String name = "reactor.txservice";

  private final CConfiguration cConf;
  private final File cConfFile;

  private final File hConfFile;

  public TransactionServiceTwillApplication(CConfiguration cConf, File cConfFile, File hConfFile) {
    this.cConf = cConf;
    this.cConfFile = cConfFile;
    this.hConfFile = hConfFile;
  }

  @Override
  public TwillSpecification configure() {
    int numCores = cConf.getInt(Constants.Gateway.NUM_CORES, Constants.Gateway.DEFAULT_NUM_CORES);
    int memoryMb = cConf.getInt(Constants.Gateway.MEMORY_MB, Constants.Gateway.DEFAULT_MEMORY_MB);
    int instances = cConf.getInt(Constants.Gateway.NUM_INSTANCES, Constants.Gateway.DEFAULT_NUM_INSTANCES);

    // It is always present in continuuity-default.xml
    long noContainerTimeout = cConf.getLong(Constants.CFG_TWILL_NO_CONTAINER_TIMEOUT, Long.MAX_VALUE);

    ResourceSpecification spec = ResourceSpecification.Builder
      .with()
      .setVirtualCores(numCores)
      .setMemory(memoryMb, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    return TwillSpecification.Builder.with()
      .setName(name)
      .withRunnable()
      .add(new TransactionServiceTwillRunnable("txservice", "cConf.xml", "hConf.xml"), spec)
      .withLocalFiles()
      .add("cConf.xml", cConfFile.toURI())
      .add("hConf.xml", hConfFile.toURI())
      .apply().anyOrder().withEventHandler(new AbortOnTimeoutEventHandler(noContainerTimeout)).build();
  }
}
