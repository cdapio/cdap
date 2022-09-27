/*
 * Copyright © 2014-2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.cdap.common.twill;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DistributedProgramContainerModule;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.internal.app.program.ProgramStateWriterWithHeartBeat;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.EventHandlerContext;
import org.apache.twill.api.RunId;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Twill {@link EventHandler} that responds to Twill application lifecycle events and aborts the application if
 * it cannot provision container for some runnable.
 */
public class TwillAppLifecycleEventHandler extends AbortOnTimeoutEventHandler {

  private static final Gson GSON = new Gson();
  private static final String HADOOP_CONF_FILE_NAME = "hConf.xml";
  private static final String CDAP_CONF_FILE_NAME = "cConf.xml";
  private static final Logger LOG = LoggerFactory.getLogger(TwillAppLifecycleEventHandler.class);

  private RunId twillRunId;
  private ClusterMode clusterMode;
  private ProgramRunId programRunId;
  private RuntimeMonitorType runtimeMonitorType;
  private ProgramStateWriterWithHeartBeat programStateWriterWithHeartBeat;
  private ZKClientService zkClientService;
  private AtomicBoolean runningPublished;
  private ContainerFailure lastContainerFailure;
  private ProgramOptions programOptions;

  /**
   * Constructs an instance of TwillAppLifecycleEventHandler that abort the application if some runnable has not enough
   * containers.
   * @param abortTime Time in milliseconds to pass before aborting the application if no container is given to
   *                  a runnable.
   * @param abortIfNotFull If {@code true}, it will abort the application if any runnable doesn't meet the expected
   *                       number of instances.
   * @param programRunId the program run id that this event handler is handling
   */
  public TwillAppLifecycleEventHandler(long abortTime, boolean abortIfNotFull, ProgramRunId programRunId,
                                       ClusterMode clusterMode, RuntimeMonitorType runtimeMonitorType,
                                       ProgramOptions programOptions) {
    super(abortTime, abortIfNotFull);
    this.programRunId = programRunId;
    this.clusterMode = clusterMode;
    this.runtimeMonitorType = runtimeMonitorType;
    this.programOptions = programOptions;
  }

  private static <T> T readJsonFile(File file, Class<T> type) {
    try (Reader reader = new BufferedReader(new FileReader(file))) {
      return GSON.fromJson(reader, type);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to read %s file at %s", type.getSimpleName(), file.getAbsolutePath()), e);
    }
  }

  @Override
  protected Map<String, String> getConfigs() {
    Map<String, String> configs = new HashMap<>(super.getConfigs());
    configs.put("programRunId", GSON.toJson(programRunId));
    configs.put("clusterMode", clusterMode.name());
    configs.put("monitorType", runtimeMonitorType.name());
    return configs;
  }

  @Override
  public void initialize(EventHandlerContext context) {
    super.initialize(context);

    LOG.warn("In TwillAppLifecycleEventHandler initialize {}", this);
    this.runningPublished = new AtomicBoolean();
    this.twillRunId = context.getRunId();

    Map<String, String> configs = context.getSpecification().getConfigs();
    this.programRunId = GSON.fromJson(configs.get("programRunId"), ProgramRunId.class);
    this.clusterMode = ClusterMode.valueOf(configs.get("clusterMode"));
    this.runtimeMonitorType = RuntimeMonitorType.valueOf(configs.get("monitorType"));

    // Fetch cConf and hConf from resources jar
    File cConfFile = new File("resources.jar/resources/" + CDAP_CONF_FILE_NAME);
    File hConfFile = new File("resources.jar/resources/" + HADOOP_CONF_FILE_NAME);
//    ProgramOptions programOpts = readJsonFile(new File(DistributedProgramRunner.PROGRAM_OPTIONS_FILE_NAME),
//                                              ProgramOptions.class);

    if (!cConfFile.exists()) {
      // This shouldn't happen, unless CDAP is misconfigured
      throw new IllegalArgumentException("Missing cConf file " + cConfFile.getAbsolutePath());
    }

    try {
      // Load the configuration from the XML files serialized from the cdap master.
      CConfiguration cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(cConfFile.toURI().toURL());

      Configuration hConf = new Configuration();
      if (hConfFile.exists()) {
        hConf.clear();
        hConf.addResource(hConfFile.toURI().toURL());
      }

      // Create the injector to create a program state writer
//      List<Module> modules = new ArrayList<>(Arrays.asList(
//        new ConfigModule(cConf, hConf),
//        RemoteAuthenticatorModules.getDefaultModule(),
//        new MessagingClientModule(),
//        new AbstractModule() {
//          @Override
//          protected void configure() {
//            bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);
//          }
//        }
//      ));

      List<Module> modules = new ArrayList<>();
      for (Map.Entry<String, String> entry : configs.entrySet()) {
        LOG.warn("Entry key = " + entry.getKey() + " Entry Value = " + entry.getValue());
      }
      if (programOptions == null) {
        LOG.warn("Program Options are null");
      }

      switch (clusterMode) {
        case ON_PREMISE:
          modules.add(new AuthenticationContextModules().getProgramContainerModule(cConf));
          modules.add(new ZKClientModule());
          modules.add(new ZKDiscoveryModule());
          modules.add(new KafkaClientModule());
          break;
        case ISOLATED:
          modules.add(new DistributedProgramContainerModule(cConf, hConf, programRunId, programOptions));
          //modules.add(new AuthenticationContextModules().getProgramContainerModule(cConf));
          //modules.add(new RemoteExecutionDiscoveryModule());
//          modules.add(new AbstractModule() {
//            @Override
//            protected void configure() {
//              bind(RuntimeMonitorType.class).toInstance(runtimeMonitorType);
//            }
//          });
          break;
      }

      Injector injector = Guice.createInjector(modules);
      // Setup logging context for the program
      LoggingContextAccessor.setLoggingContext(LoggingContextHelper.getLoggingContextWithRunId(programRunId, null));
      LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();

      if (clusterMode == ClusterMode.ON_PREMISE) {
        zkClientService = injector.getInstance(ZKClientService.class);
        zkClientService.startAndWait();
      }

      ProgramStateWriter programStateWriter = injector.getInstance(ProgramStateWriter.class);
      MessagingService messagingService = injector.getInstance(MessagingService.class);
      programStateWriterWithHeartBeat =
        new ProgramStateWriterWithHeartBeat(programRunId, programStateWriter, messagingService, cConf);
      LOG.warn("Successfully initialized TwillAppLifecycleEventHandler");
    } catch (Exception e) {
      LOG.error("Failed to initialize TwillAppLifecycleEventHandler");
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void containerLaunched(String runnableName, int instanceId, String containerId) {
    super.containerLaunched(runnableName, instanceId, containerId);

    if (runningPublished.compareAndSet(false, true)) {
      // The program is marked as running when the first container for the program is launched
      programStateWriterWithHeartBeat.running(twillRunId.getId());
    }
  }

  @Override
  public void completed() {
    super.completed();
    // On normal AM completion, based on the last container failure to publish the state
    if (lastContainerFailure == null) {
      programStateWriterWithHeartBeat.completed();
    } else {
      lastContainerFailure.writeError(programStateWriterWithHeartBeat);
    }
  }

  @Override
  public void killed() {
    super.killed();
    // The AM is stopped explicitly, always record the state as killed.
    programStateWriterWithHeartBeat.killed();
  }

  @Override
  public void containerStopped(String runnableName, int instanceId, String containerId, int exitStatus) {
    super.containerStopped(runnableName, instanceId, containerId, exitStatus);

    // Let the completed() method handle when a container has completed with no error
    if (exitStatus == 0) {
      return;
    }

    switch(programRunId.getType()) {
      case WORKFLOW:
      case SPARK:
      case MAPREDUCE:
        // For workflow, MapReduce, and spark, if there is an error, the program state is failure
        // We defer the actual publish to one of the completion methods (killed, completed, aborted)
        // as we need to know under what condition the container failed.
        lastContainerFailure = new ContainerFailure(runnableName, instanceId, containerId, exitStatus);
        break;
      default:
        // For other programs, the container will be re-launched - the program state will continue to be RUNNING
        // TODO Workers should be configured via runtime args
        // to support both retrying on failure, or just failing and not retrying.
        break;
    }
  }

  @Override
  public void aborted() {
    super.aborted();
    programStateWriterWithHeartBeat.error(
      new Exception(String.format("No containers for %s. Abort the application", programRunId)));
  }

  @Override
  public void destroy() {
    if (zkClientService != null) {
      zkClientService.stop();
    }
  }

  /**
   * Inner class for hold failure information provided to the {@link #containerStopped(String, int, String, int)}
   * method.
   */
  private static final class ContainerFailure {
    private final String runnableName;
    private final int instanceId;
    private final String containerId;
    private final int exitStatus;

    ContainerFailure(String runnableName, int instanceId, String containerId, int exitStatus) {
      this.runnableName = runnableName;
      this.instanceId = instanceId;
      this.containerId = containerId;
      this.exitStatus = exitStatus;
    }

    void writeError(ProgramStateWriterWithHeartBeat writer) {
      String errorMessage = String.format("Container %s of Runnable %s with instance %s stopped with exit status %d",
                                          containerId, runnableName, instanceId, exitStatus);
      writer.error(new Exception(errorMessage));
    }
  }
}
