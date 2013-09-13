package com.continuuity.gateway.v2.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.weave.api.WeaveApplication;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 * Run Gateway using weave.
 */
public class GatewayWeaveRunnerMain extends WeaveRunnerMain {
  private final CConfiguration cConf;

  public GatewayWeaveRunnerMain(CConfiguration cConf) {
    this.cConf = cConf;
  }

  public static void main(String[] args) throws Exception {
    new GatewayWeaveRunnerMain(CConfiguration.create()).doMain(args);
  }

  @Override
  protected WeaveApplication createWeaveApplication() {
    try {
      File cConfFile;
      cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
      cConfFile.deleteOnExit();

      return new GatewayWeaveApplication(cConf, cConfFile);

    } catch (Exception e) {
      throw  Throwables.propagate(e);
    }
  }

  @Override
  protected CConfiguration getConfiguration() {
    return cConf;
  }

  private File saveCConf(CConfiguration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
  }
}
