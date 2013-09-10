/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.serialize.LogSchema;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.WeaveRunnerService;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.continuuity.weave.common.ServiceListenerAdapter;
import com.continuuity.weave.filesystem.LocationFactories;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.yarn.YarnWeaveRunnerService;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Iterator;

/**
 * Wrapper class to run LogSaver as a process.
 */
public final class LogSaverMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverMain.class);

  private WeaveRunnerService weaveRunnerService;
  private WeaveController weaveController;

  private Configuration hConf;
  private CConfiguration cConf;
  private String yarnUser;

  public static void main(String [] args) throws Exception {
    new LogSaverMain().doMain(args);
  }

  @Override
  public void init(String[] args) {
    hConf = new Configuration();
    cConf = CConfiguration.create();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(WeaveRunnerService.class).to(YarnWeaveRunnerService.class);
          bind(WeaveRunner.class).to(WeaveRunnerService.class);
        }

        @Singleton
        @Provides
        private YarnWeaveRunnerService provideYarnWeaveRunnerService(CConfiguration configuration,
                                                                     YarnConfiguration yarnConfiguration,
                                                                     LocationFactory locationFactory) {
          String zkNamespace = configuration.get(Constants.CFG_WEAVE_ZK_NAMESPACE, "/weave");
          return new YarnWeaveRunnerService(yarnConfiguration,
                                            configuration.get(Constants.Zookeeper.QUORUM) + zkNamespace,
                                            LocationFactories.namespace(locationFactory, "weave"));
        }
      }
    );

    weaveRunnerService = injector.getInstance(WeaveRunnerService.class);
    yarnUser = cConf.get(Constants.CFG_YARN_USER, System.getProperty("user.name"));
  }

  @Override
  public void start() {
    weaveRunnerService.startAndWait();

    // If LogSaver is already running, return handle to that instance
    Iterable<WeaveController> weaveControllers = weaveRunnerService.lookup(LogSaverWeaveApplication.getName());
    Iterator<WeaveController> iterator = weaveControllers.iterator();

    if (iterator.hasNext()) {
      LOG.info("{} application is already running", LogSaverWeaveApplication.getName());
      weaveController = iterator.next();

      if (iterator.hasNext()) {
        LOG.warn("Found more than one instance of {} running. Stopping the others...",
                 LogSaverWeaveApplication.getName());
        for (; iterator.hasNext(); ) {
          WeaveController controller = iterator.next();
          LOG.warn("Stopping one extra instance of {}", LogSaverWeaveApplication.getName());
          controller.stopAndWait();
        }
        LOG.warn("Done stopping extra instances of {}", LogSaverWeaveApplication.getName());
      }
    } else {
      LOG.info("Starting {} application", LogSaverWeaveApplication.getName());
      WeavePreparer weavePreparer = doInit(weaveRunnerService, hConf, cConf);
      weaveController = weavePreparer.start();

      weaveController.addListener(new ServiceListenerAdapter() {
        @Override
        public void failed(Service.State from, Throwable failure) {
          LOG.error("{} failed with exception... stopping LogSaverMain.", LogSaverWeaveApplication.getName(), failure);
          System.exit(1);
        }
      }, MoreExecutors.sameThreadExecutor());
    }
  }

  @Override
  public void stop() {
    LOG.info("Stopping {}", LogSaverWeaveApplication.getName());
    if (weaveController != null && weaveController.isRunning()) {
      weaveController.stopAndWait();
    }
  }

  @Override
  public void destroy() {
    LOG.info("Destroying {}", LogSaverWeaveApplication.getName());
    if (weaveRunnerService != null && weaveRunnerService.isRunning()) {
      weaveRunnerService.stopAndWait();
    }
  }

  private WeavePreparer doInit(WeaveRunner weaveRunner, Configuration hConf, CConfiguration cConf) {
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
        .setUser(yarnUser)
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
