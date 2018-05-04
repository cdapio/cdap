/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.common.twill;

import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.internal.app.program.MessagingProgramStateWriter;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.EventHandlerContext;
import org.apache.twill.api.RunId;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Twill {@link EventHandler} that responds to Twill application lifecycle events and aborts the application if
 * it cannot provision container for some runnable.
 */
public class TwillAppLifecycleEventHandler extends AbortOnTimeoutEventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TwillAppLifecycleEventHandler.class);
  private static final Gson GSON = new Gson();
  private static final String HADOOP_CONF_FILE_NAME = "hConf.xml";
  private static final String CDAP_CONF_FILE_NAME = "cConf.xml";

  private RunId twillRunId;
  private ProgramRunId programRunId;
  private ProgramStateWriter programStateWriter;
  private ZKClientService zkClientService;
  private AtomicBoolean runningPublished;
  private ContainerFailure lastContainerFailure;

  /**
   * Constructs an instance of TwillAppLifecycleEventHandler that abort the application if some runnable has not enough
   * containers.
   * @param abortTime Time in milliseconds to pass before aborting the application if no container is given to
   *                  a runnable.
   * @param abortIfNotFull If {@code true}, it will abort the application if any runnable doesn't meet the expected
   *                       number of instances.
   * @param programRunId the program run id that this event handler is handling
   */
  public TwillAppLifecycleEventHandler(long abortTime, boolean abortIfNotFull, ProgramRunId programRunId) {
    super(abortTime, abortIfNotFull);
    this.programRunId = programRunId;
  }

  @Override
  protected Map<String, String> getConfigs() {
    Map<String, String> configs = new HashMap<>(super.getConfigs());
    configs.put("programRunId", GSON.toJson(programRunId));
    return configs;
  }

  @Override
  public void initialize(EventHandlerContext context) {
    super.initialize(context);

    this.runningPublished = new AtomicBoolean();
    this.twillRunId = context.getRunId();
    this.programRunId = GSON.fromJson(context.getSpecification().getConfigs().get("programRunId"), ProgramRunId.class);

    // Fetch cConf and hConf from resources jar
    File cConfFile = new File("resources.jar/resources/" + CDAP_CONF_FILE_NAME);
    File hConfFile = new File("resources.jar/resources/" + HADOOP_CONF_FILE_NAME);

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
      Injector injector = Guice.createInjector(
        new ConfigModule(cConf, hConf),
        new ZKClientModule(),
        new KafkaClientModule(),
        new DiscoveryRuntimeModule().getDistributedModules(),
        new MessagingClientModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class);
          }
        }
      );

      zkClientService = injector.getInstance(ZKClientService.class);
      zkClientService.startAndWait();

      programStateWriter = injector.getInstance(ProgramStateWriter.class);

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void containerLaunched(String runnableName, int instanceId, String containerId) {
    super.containerLaunched(runnableName, instanceId, containerId);

    if (runningPublished.compareAndSet(false, true)) {
      // The program is marked as running when the first container for the program is launched
      programStateWriter.running(programRunId, twillRunId.getId());
    }
  }

  @Override
  public void completed() {
    super.completed();
    // On normal AM completion, based on the last container failure to publish the state
    if (lastContainerFailure == null) {
      programStateWriter.completed(programRunId);
    } else {
      lastContainerFailure.writeError(programStateWriter, programRunId);
    }
  }

  @Override
  public void killed() {
    super.killed();
    // The AM is stopped explicitly, always record the state as killed.
    programStateWriter.killed(programRunId);
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
    programStateWriter.error(programRunId,
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

    void writeError(ProgramStateWriter writer, ProgramRunId programRunId) {
      String errorMessage = String.format("Container %s, instance %s stopped with exit status %d",
                                          containerId, instanceId, exitStatus);
      writer.error(programRunId, new Exception(errorMessage));
    }
  }
}
