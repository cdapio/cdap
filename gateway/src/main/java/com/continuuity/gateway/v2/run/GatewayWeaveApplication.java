package com.continuuity.gateway.v2.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 * WeaveApplication wrapper for Gateway.
 */
@SuppressWarnings("UnusedDeclaration")
public class GatewayWeaveApplication implements WeaveApplication {
  public final String name;

  public GatewayWeaveApplication(String name) {
    this.name = name;
  }

  @Override
  public WeaveSpecification configure() {
    try {
      CConfiguration cConf = CConfiguration.create();
      int numCores = cConf.getInt(Constants.Gateway.NUM_CORES, Constants.Gateway.DEFAULT_NUM_CORES);
      int memoryMb = cConf.getInt(Constants.Gateway.MEMORY_MB, Constants.Gateway.DEFAULT_MEMORY_MB);
      int instances = cConf.getInt(Constants.Gateway.NUM_INSTANCES, Constants.Gateway.DEFAULT_NUM_INSTANCES);

      File cConfFile;
      cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
      cConfFile.deleteOnExit();

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
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static File saveCConf(CConfiguration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }
}
