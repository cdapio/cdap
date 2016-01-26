/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.logging.KafkaAppender;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.StringReader;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Class for main method that starts a service.
 * TODO: This is copied from TWILL-154 and has temp fix for TWILL-147 for MapR issue
 * and should be removed when upgrade to Twill 0.8.0
 */
public abstract class ServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceMain.class);

  static {
    // This is to work around detection of HADOOP_HOME (HADOOP-9422)
    if (!System.getenv().containsKey("HADOOP_HOME") && System.getProperty("hadoop.home.dir") == null) {
      System.setProperty("hadoop.home.dir", new File("").getAbsolutePath());
    }
  }

  protected final void doMain(final Service mainService,
                              Service...prerequisites) throws ExecutionException, InterruptedException {
    if (Boolean.parseBoolean(System.getProperty("twill.disable.kafka"))) {
      LOG.info("Log collection through kafka disabled");
    } else {
      configureLogger();
    }

    Service requiredServices = new CompositeService(prerequisites);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        mainService.stopAndWait();
      }
    });

    // Listener for state changes of the service
    ListenableFuture<Service.State> completion = Services.getCompletionFuture(mainService);
    Throwable initFailure = null;

    try {
      try {
        // Starts the service
        LOG.info("Starting service {}.", mainService);
        Futures.allAsList(Services.chainStart(requiredServices, mainService).get()).get();
        LOG.info("Service {} started.", mainService);
      } catch (Throwable t) {
        LOG.error("Exception when starting service {}.", mainService, t);
        initFailure = t;
      }

      try {
        if (initFailure == null) {
          completion.get();
          LOG.info("Service {} completed.", mainService);
        }
      } catch (Throwable t) {
        LOG.error("Exception thrown from service {}.", mainService, t);
        throw Throwables.propagate(t);
      }
    } finally {
      requiredServices.stopAndWait();

      ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
      if (loggerFactory instanceof LoggerContext) {
        ((LoggerContext) loggerFactory).stop();
      }

      if (initFailure != null) {
        // Exit with the init fail exit code.
        System.exit(ContainerExitCodes.INIT_FAILED);
      }
    }
  }

  protected abstract String getHostname();

  protected abstract String getKafkaZKConnect();

  protected abstract String getRunnableName();

  /**
   * Returns the {@link Location} for the application based on the env {@link EnvKeys#TWILL_APP_DIR}.
   */
  protected static Location createAppLocation(Configuration conf) {
    // Note: It's a little bit hacky based on the uri schema to create the LocationFactory, refactor it later.
    URI appDir = URI.create(System.getenv(EnvKeys.TWILL_APP_DIR));

    try {
      if ("file".equals(appDir.getScheme())) {
        return new LocalLocationFactory().create(appDir);
      }

      // If not file, assuming it is a FileSystem, hence construct with HDFSLocationFactory which wraps
      // a FileSystem created from the Configuration
      if (UserGroupInformation.isSecurityEnabled()) {
        return new HDFSLocationFactory(FileSystem.get(appDir, conf)).create(appDir);
      }

      String fsUser = System.getenv(EnvKeys.TWILL_FS_USER);
      if (fsUser == null) {
        throw new IllegalStateException("Missing environment variable " + EnvKeys.TWILL_FS_USER);
      }
      return new HDFSLocationFactory(FileSystem.get(appDir, conf, fsUser)).create(appDir);

    } catch (Exception e) {
      LOG.error("Failed to create application location for {}.", appDir);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a {@link ZKClientService}.
   */
  protected static ZKClientService createZKClient(String zkConnectStr, String appName) {
    return ZKClientServices.delegate(
      ZKClients.namespace(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(
            ZKClientService.Builder.of(zkConnectStr).build(),
            RetryStrategies.fixDelay(1, TimeUnit.SECONDS)
          )
        ), "/" + appName
      ));
  }

  private void configureLogger() {
    // Check if SLF4J is bound to logback in the current environment
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      return;
    }

    LoggerContext context = (LoggerContext) loggerFactory;
    context.reset();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);

    try {
      File twillLogback = new File(Constants.Files.LOGBACK_TEMPLATE);
      if (twillLogback.exists()) {
        configurator.doConfigure(twillLogback);
      }
      new ContextInitializer(context).autoConfig();
    } catch (JoranException e) {
      throw Throwables.propagate(e);
    }
    doConfigure(configurator, getLogConfig(getLoggerLevel(context.getLogger(Logger.ROOT_LOGGER_NAME))));
  }

  private void doConfigure(JoranConfigurator configurator, String config) {
    try {
      configurator.doConfigure(new InputSource(new StringReader(config)));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getLogConfig(String rootLevel) {
    return
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<configuration>\n" +
        "    <appender name=\"KAFKA\" class=\"" + KafkaAppender.class.getName() + "\">\n" +
        "        <topic>" + Constants.LOG_TOPIC + "</topic>\n" +
        "        <hostname>" + getHostname() + "</hostname>\n" +
        "        <zookeeper>" + getKafkaZKConnect() + "</zookeeper>\n" +
        appendRunnable() +
        "    </appender>\n" +
        "    <logger name=\"org.apache.twill.internal.logging\" additivity=\"false\" />\n" +
        "    <root level=\"" + rootLevel + "\">\n" +
        "        <appender-ref ref=\"KAFKA\"/>\n" +
        "    </root>\n" +
        "</configuration>";
  }


  private String appendRunnable() {
    // RunnableName for AM is null, so append runnable name to log config only if the name is not null.
    if (getRunnableName() == null) {
      return "";
    } else {
      return "        <runnableName>" + getRunnableName() + "</runnableName>\n";
    }
  }

  /**
   * Override to return the right log level for the service.
   *
   * @param logger the {@link Logger} instance of the service context.
   * @return String of log level based on {@code slf4j} log levels.
   */
  protected String getLoggerLevel(Logger logger) {
    if (logger instanceof ch.qos.logback.classic.Logger) {
      return ((ch.qos.logback.classic.Logger) logger).getLevel().toString();
    }

    if (logger.isTraceEnabled()) {
      return "TRACE";
    }
    if (logger.isDebugEnabled()) {
      return "DEBUG";
    }
    if (logger.isInfoEnabled()) {
      return "INFO";
    }
    if (logger.isWarnEnabled()) {
      return "WARN";
    }
    if (logger.isErrorEnabled()) {
      return "ERROR";
    }
    return "OFF";
  }

  /**
   * A simple service for creating/remove ZK paths needed for {@link AbstractTwillService}.
   */
  protected static class TwillZKPathService extends AbstractIdleService {

    protected static final long TIMEOUT_SECONDS = 5L;

    private static final Logger LOG = LoggerFactory.getLogger(TwillZKPathService.class);

    private final ZKClient zkClient;
    private final String path;

    public TwillZKPathService(ZKClient zkClient, RunId runId) {
      this.zkClient = zkClient;
      this.path = "/" + runId.getId();
    }

    @Override
    protected void startUp() throws Exception {
      LOG.info("Creating container ZK path: {}{}", zkClient.getConnectString(), path);
      ZKOperations.ignoreError(zkClient.create(path, null, CreateMode.PERSISTENT),
                               KeeperException.NodeExistsException.class, null).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
      LOG.info("Removing container ZK path: {}{}", zkClient.getConnectString(), path);
      ZKOperations.recursiveDelete(zkClient, path).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
  }
}
