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
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DistributedProgramContainerModule;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.internal.app.program.ProgramStateWriterWithHeartBeat;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.EventHandlerContext;
import org.apache.twill.api.RunId;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * A Twill {@link EventHandler} that responds to Twill application lifecycle events and aborts the
 * application if it cannot provision container for some runnable.
 */
public class TwillAppLifecycleEventHandler extends AbortOnTimeoutEventHandler {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
      .create();

  private RunId twillRunId;
  private ProgramRunId programRunId;
  private ProgramStateWriterWithHeartBeat programStateWriterWithHeartBeat;
  private ZKClientService zkClientService;
  private AtomicBoolean runningPublished;
  private ContainerFailure lastContainerFailure;
  private LogAppenderInitializer logAppenderInitializer;

  /**
   * Constructs an instance of TwillAppLifecycleEventHandler that abort the application if some
   * runnable has not enough containers.
   *
   * @param abortTime Time in milliseconds to pass before aborting the application if no
   *     container is given to a runnable.
   * @param abortIfNotFull If {@code true}, it will abort the application if any runnable
   *     doesn't meet the expected number of instances.
   */
  public TwillAppLifecycleEventHandler(long abortTime, boolean abortIfNotFull) {
    super(abortTime, abortIfNotFull);
  }

  @Override
  public void initialize(EventHandlerContext context) {
    super.initialize(context);

    this.runningPublished = new AtomicBoolean();
    this.twillRunId = context.getRunId();

    // Fetch cConf and hConf from resources jar
    File cConfFile = new File(
        "resources.jar/resources/" + DistributedProgramRunner.CDAP_CONF_FILE_NAME);
    File hConfFile = new File(
        "resources.jar/resources/" + DistributedProgramRunner.HADOOP_CONF_FILE_NAME);

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

      ProgramOptions programOpts = createProgramOptions(
          new File(
              "resources.jar/resources/" + DistributedProgramRunner.PROGRAM_OPTIONS_FILE_NAME));
      ClusterMode clusterMode = ProgramRunners.getClusterMode(programOpts);
      programRunId = programOpts.getProgramId().run(ProgramRunners.getRunId(programOpts));

      // Create the injector to create a program state writer
      Injector injector = Guice.createInjector(new DistributedProgramContainerModule(cConf, hConf,
          programRunId, programOpts));

      if (clusterMode == ClusterMode.ON_PREMISE) {
        zkClientService = injector.getInstance(ZKClientService.class);
        zkClientService.startAndWait();
      }

      LoggingContextAccessor.setLoggingContext(
          LoggingContextHelper.getLoggingContextWithRunId(programRunId,
              programOpts.getArguments().asMap()));

      logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();
      SystemArguments.setLogLevel(programOpts.getUserArguments(), logAppenderInitializer);

      ProgramStateWriter programStateWriter = injector.getInstance(ProgramStateWriter.class);
      MessagingService messagingService = injector.getInstance(MessagingService.class);
      programStateWriterWithHeartBeat =
          new ProgramStateWriterWithHeartBeat(programRunId, programStateWriter, messagingService,
              cConf);
    } catch (Exception e) {
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
  public void containerStopped(String runnableName, int instanceId, String containerId,
      int exitStatus) {
    super.containerStopped(runnableName, instanceId, containerId, exitStatus);

    // Let the completed() method handle when a container has completed with no error
    if (exitStatus == 0) {
      return;
    }

    switch (programRunId.getType()) {
      case WORKFLOW:
      case SPARK:
      case MAPREDUCE:
        // For workflow, MapReduce, and spark, if there is an error, the program state is failure
        // We defer the actual publish to one of the completion methods (killed, completed, aborted)
        // as we need to know under what condition the container failed.
        lastContainerFailure = new ContainerFailure(runnableName, instanceId, containerId,
            exitStatus);
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
    Closeables.closeQuietly(logAppenderInitializer);
    if (zkClientService != null) {
      zkClientService.stop();
    }
  }

  private ProgramOptions createProgramOptions(File programOptionsFile) throws IOException {
    try (Reader reader = Files.newBufferedReader(programOptionsFile.toPath(),
        StandardCharsets.UTF_8)) {
      return GSON.fromJson(reader, ProgramOptions.class);
    }
  }

  /**
   * Inner class for hold failure information provided to the {@link #containerStopped(String, int,
   * String, int)} method.
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
      String errorMessage = String.format(
          "Container %s of Runnable %s with instance %s stopped with exit status %d",
          containerId, runnableName, instanceId, exitStatus);
      writer.error(new Exception(errorMessage));
    }
  }
}
