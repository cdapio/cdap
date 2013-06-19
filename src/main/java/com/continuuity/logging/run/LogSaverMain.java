/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.logging.LoggingConfiguration;
import com.continuuity.common.logging.logback.serialize.LogSchema;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.URISyntaxException;

/**
 * Wrapper class to run LogSaver as a process.
 */
public final class LogSaverMain extends DaemonMain {
  private WeaveRunnerService weaveRunnerService;
  private WeavePreparer weavePreparer;
  private WeaveController weaveController;

  public static void main(String [] args) throws Exception {
    new LogSaverMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    Configuration hConf = new Configuration();
    CConfiguration cConf = CConfiguration.create();
    weaveRunnerService = new YarnWeaveRunnerService(new YarnConfiguration(),
                                                    cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE));
    weavePreparer = doInit(weaveRunnerService, hConf, cConf);
  }

  @Override
  public void start() {
    weaveController = weavePreparer.start();
  }

  @Override
  public void stop() {
    weaveController.stopAndWait();
  }

  @Override
  public void destroy() {
    weaveRunnerService.stopAndWait();
  }

  static WeavePreparer doInit(WeaveRunner weaveRunner, Configuration hConf, CConfiguration cConf) {
    int partitions = cConf.getInt(LoggingConfiguration.NUM_PARTITIONS,  -1);
    if (partitions < 1) {
      throw new IllegalArgumentException("log.publish.partitions should be at least 1");
    }

    File hConfFile;
    File cConfFile;
    try {
      hConfFile = saveHConf(hConf, File.createTempFile("hConf", ".xml"));
      hConfFile.deleteOnExit();
      cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
      cConfFile.deleteOnExit();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    try {
      return weaveRunner.prepare(new LogSaverWeaveApplication(partitions, hConfFile, cConfFile))
        // TODO: write logs to file
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
        .withResources(LogSchema.getSchemaURL().toURI());
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
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
