package com.continuuity.gateway.v2.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;

import java.io.File;

/**
 * WeaveApplication wrapper for Gateway.
 */
@SuppressWarnings("UnusedDeclaration")
public class GatewayWeaveApplication implements WeaveApplication {
  private static final String name = "GatewayWeaveApplication";
  private final CConfiguration cConf;
  private final File cConfFile;

  public GatewayWeaveApplication(CConfiguration cConf, File cConfFile) {
    this.cConf = cConf;
    this.cConfFile = cConfFile;
  }

  @Override
  public WeaveSpecification configure() {
    int numCores = cConf.getInt(Constants.Gateway.NUM_CORES, Constants.Gateway.DEFAULT_NUM_CORES);
    int memoryMb = cConf.getInt(Constants.Gateway.MEMORY_MB, Constants.Gateway.DEFAULT_MEMORY_MB);
    int instances = cConf.getInt(Constants.Gateway.NUM_INSTANCES, Constants.Gateway.DEFAULT_NUM_INSTANCES);

    WeaveSpecification.Builder.MoreRunnable moreRunnable = WeaveSpecification.Builder.with()
      .setName(name)
      .withRunnable();

    ResourceSpecification spec = ResourceSpecification.Builder
      .with()
      .setCores(numCores)
      .setMemory(memoryMb, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(instances)
      .build();

    WeaveSpecification.Builder.RunnableSetter runnableSetter =
      moreRunnable.add(new GatewayWeaveRunnable("GatewayWeaveRunnable", "cConf.xml"), spec)
        .withLocalFiles()
        .add("cConf.xml", cConfFile.toURI())
        .apply();

    return runnableSetter.anyOrder().build();
  }
}
