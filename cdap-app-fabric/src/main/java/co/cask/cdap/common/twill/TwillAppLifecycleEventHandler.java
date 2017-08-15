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

import co.cask.cdap.app.runtime.NoOpProgramStateWriter;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.store.DirectStoreProgramStateWriter;
import co.cask.cdap.internal.app.store.remote.RemoteRuntimeStore;
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
  private AtomicBoolean launchedContainer;
  private AtomicBoolean stoppedContainer;

  /**
   * Constructs an instance of TwillAppLifecycleEventHandler that abort the application if some runnable has no
   * containers and a program state writer to respond to Twill application lifecycle events
   *
   * @param abortTime Time in milliseconds to pass before aborting the application if no container is given to
   *                  a runnable.
   * @param options the program options, if the event handler was attached to monitor program states
   */
  public TwillAppLifecycleEventHandler(long abortTime, ProgramOptions options) {
    this(abortTime, false, options);
  }

  /**
   * Constructs an instance of TwillAppLifecycleEventHandler that abort the application if some runnable has not enough
   * containers.
   * @param abortTime Time in milliseconds to pass before aborting the application if no container is given to
   *                  a runnable.
   * @param abortIfNotFull If {@code true}, it will abort the application if any runnable doesn't meet the expected
   *                       number of instances.
   * @param options the program options, if the event handler was attached to monitor program states
   */
  public TwillAppLifecycleEventHandler(long abortTime, boolean abortIfNotFull, ProgramOptions options) {
    super(abortTime, abortIfNotFull);
    this.programRunId = options.getProgramId().run(ProgramRunners.getRunId(options));
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

    this.twillRunId = context.getRunId();
    this.programRunId = GSON.fromJson(context.getSpecification().getConfigs().get("programRunId"), ProgramRunId.class);

    // Fetch cConf and hConf from resources jar
    File cConfFile = new File("resources.jar/resources/" + CDAP_CONF_FILE_NAME);
    File hConfFile = new File("resources.jar/resources/" + HADOOP_CONF_FILE_NAME);

    if (cConfFile.exists() && hConfFile.exists()) {
      CConfiguration cConf = CConfiguration.create();
      cConf.clear();

      Configuration hConf = new Configuration();
      hConf.clear();

      try {
        cConf.addResource(cConfFile.toURI().toURL());
        hConf.addResource(hConfFile.toURI().toURL());

        // Create the injector to inject a program state writer
        Injector injector = Guice.createInjector(
          new ConfigModule(cConf, hConf),
          new ZKClientModule(),
          new KafkaClientModule(),
          new DiscoveryRuntimeModule().getDistributedModules(),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(RuntimeStore.class).to(RemoteRuntimeStore.class);
              bind(ProgramStateWriter.class).to(DirectStoreProgramStateWriter.class);
            }
          }
        );

        zkClientService = injector.getInstance(ZKClientService.class);
        startServices();
        this.programStateWriter = injector.getInstance(ProgramStateWriter.class);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    } else {
      LOG.warn("{} and {} were not found in the resources.jar. Not recording program states",
               CDAP_CONF_FILE_NAME, HADOOP_CONF_FILE_NAME);
      this.programStateWriter = new NoOpProgramStateWriter();
    }
  }

  @Override
  public void started() {
    super.started();
    this.launchedContainer = new AtomicBoolean(false);
    this.stoppedContainer = new AtomicBoolean(false);
  }

  @Override
  public void containerLaunched(String runnableName, int instanceId, String containerId) {
    super.containerLaunched(runnableName, instanceId, containerId);

    if (!launchedContainer.compareAndSet(false, true)) {
      return;
    }
    // The program is marked as running when the first container for the program is launched
    programStateWriter.running(programRunId, twillRunId.getId());
  }

  @Override
  public void completed() {
    super.completed();

    // If we already stopped the container due to an error, just return
    if (stoppedContainer.get()) {
      return;
    }
    programStateWriter.completed(programRunId);
  }

  @Override
  public void killed() {
    super.killed();
    programStateWriter.killed(programRunId);
  }

  @Override
  public void containerStopped(String runnableName, int instanceId, String containerId, int exitStatus) {
    super.containerStopped(runnableName, instanceId, containerId, exitStatus);

    // Let the completed handler handle when a container has completed with no error
    if (exitStatus == 0) {
      return;
    }

    String errorMessage = String.format("Container %s, instance %s stopped with exit status %d",
                                        containerId, instanceId, exitStatus);
    LOG.error(errorMessage);

    switch(programRunId.getType()) {
      case WORKFLOW:
      case SPARK:
      case MAPREDUCE:
        // For workflow, mapreduce, and spark, if there is an error, record the error state
        if (stoppedContainer.compareAndSet(false, true)) {
          programStateWriter.error(programRunId, new RuntimeException(errorMessage));
        }
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
                             new Exception(String.format("No containers for {}. Abort the application", programRunId)));
  }

  private void startServices() {
    if (zkClientService != null) {
      zkClientService.startAndWait();
    }
  }

  @Override
  public void destroy() {
    if (zkClientService != null) {
      zkClientService.stop();
    }
  }
}
