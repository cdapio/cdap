/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.serialize.LogSchema;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

/**
 * Wrapper class to run LogSaver as a process.
 */
public final class LogSaverMain extends DaemonMain {
  private WeaveRunnerService weaveRunnerService;
  private WeaveController weaveController;

  private Configuration hConf;
  private CConfiguration cConf;

  public static void main(String [] args) throws Exception {
    new LogSaverMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    hConf = new Configuration();
    cConf = CConfiguration.create();
    weaveRunnerService = new YarnWeaveRunnerService(new YarnConfiguration(),
                                                    cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE));
  }

  @Override
  public void start() {
    weaveRunnerService.startAndWait();
    WeavePreparer weavePreparer = doInit(weaveRunnerService, hConf, cConf);
    weaveController = weavePreparer.start();
  }

  @Override
  public void stop() {
    if (weaveController != null) {
      weaveController.stopAndWait();
    }
  }

  @Override
  public void destroy() {
    if (weaveRunnerService != null) {
      weaveRunnerService.stopAndWait();
    }
  }

  static WeavePreparer doInit(WeaveRunner weaveRunner, Configuration hConf, CConfiguration cConf) {
    int partitions = cConf.getInt(LoggingConfiguration.NUM_PARTITIONS,  -1);
    Preconditions.checkArgument(partitions > 0, "log.publish.partitions should be at least 1, got %s", partitions);

    int memory = cConf.getInt(LoggingConfiguration.LOG_SAVER_RUN_MEMORY_MB, 1024);
    Preconditions.checkArgument(memory > 0, "Got invalid memory value for log saver %s", memory);

    File hConfFile;
    File cConfFile;
    try {
      hConfFile = saveHConf(hConf, File.createTempFile("hConf", ".xml"));
      hConfFile.deleteOnExit();
      cConfFile = saveCConf(cConf, File.createTempFile("cConf", ".xml"));
      cConfFile.deleteOnExit();

      return weaveRunner.prepare(new LogSaverWeaveApplication(partitions, memory, hConfFile, cConfFile))
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
        .withResources(LogSchema.getSchemaURL().toURI());
    } catch (Exception e) {
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
