package com.continuuity.gateway.v2.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.weave.api.WeaveApplication;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

/**
 * Run Gateway using weave.
 */
public class GatewayWeaveRunnerMain extends WeaveRunnerMain {
  private final CConfiguration cConf;
  private final Configuration hConf;

  public GatewayWeaveRunnerMain(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  public static void main(String[] args) throws Exception {
    new GatewayWeaveRunnerMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
  }

  @Override
  protected WeaveApplication createWeaveApplication() {
    try {
      File hConfFile = saveHConf(hConf, File.createTempFile("hConf", ".xml"));
      hConfFile.deleteOnExit();

      File cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
      cConfFile.deleteOnExit();

      return new GatewayWeaveApplication(cConf, cConfFile, hConfFile);

    } catch (Exception e) {
      throw  Throwables.propagate(e);
    }
  }

  @Override
  protected CConfiguration getConfiguration() {
    return cConf;
  }

  private static File saveHConf(Configuration conf, File file) throws IOException {
    Writer writer = Files.newWriter(file, Charsets.UTF_8);
    try {
      conf.writeXml(writer);
    } finally {
      writer.close();
    }
    return file;
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
