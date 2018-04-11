/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.guice.DistributedProgramStreamModule;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.Command;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * A {@link TwillRunnable} for running a program through a {@link ProgramRunner}.
 *
 * @param <T> The {@link ProgramRunner} type.
 */
public abstract class AbstractProgramTwillRunnable<T extends ProgramRunner> extends DistributedProgramContainer<T>
                                                                            implements TwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramTwillRunnable.class);

  protected String name;
  private TwillContext context;
  private ProgramController controller;

  /**
   * Constructor.
   *
   * @param name Name of the TwillRunnable
   */
  protected AbstractProgramTwillRunnable(String name) {
    this.name = name;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .noConfigs()
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    this.context = context;
    this.name = context.getSpecification().getName();

    LOG.info("Initializing runnable: " + name);

    try {
      initialize(context.getApplicationArguments());
      LOG.info("Runnable initialized: {}", name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void run() {
    try {
      SettableFuture<ProgramController.State> state = SettableFuture.create();
      controller = executeProgram();
      controller.addListener(new AbstractListener() {

        @Override
        public void init(ProgramController.State currentState, @Nullable Throwable cause) {
          switch (currentState) {
            case COMPLETED:
              completed();
              break;
            case KILLED:
              killed();
              break;
            case ERROR:
              error(cause);
              break;
          }
        }

        @Override
        public void completed() {
          state.set(ProgramController.State.COMPLETED);
        }

        @Override
        public void killed() {
          state.set(ProgramController.State.KILLED);
        }

        @Override
        public void error(Throwable cause) {
          state.setException(cause);
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      // Block on the completion
      state.get();
    } catch (InterruptedException e) {
      LOG.warn("Program {} interrupted.", name, e);
    } catch (ExecutionException e) {
      LOG.error("Program {} execution failed.", name, e);
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    // need to make sure controller exists before handling the command
    if (ProgramCommands.SUSPEND.equals(command)) {
      controller.suspend().get();
      return;
    }
    if (ProgramCommands.RESUME.equals(command)) {
      controller.resume().get();
      return;
    }
    if (ProgramOptionConstants.INSTANCES.equals(command.getCommand())) {
      int instances = Integer.parseInt(command.getOptions().get("count"));
      controller.command(ProgramOptionConstants.INSTANCES, instances).get();
      return;
    }
    LOG.warn("Ignore unsupported command: " + command);
  }

  @Override
  public void stop() {
    try {
      LOG.info("Stopping runnable: {}.", name);
      if (controller != null) {
        controller.stop().get();
      }
    } catch (Exception e) {
      LOG.error("Failed to stop runnable: {}.", name, e);
      throw Throwables.propagate(e);
    }
  }


  @Override
  public void destroy() {
    // no-op
  }

  @Override
  protected List<Service> createCoreServices(Injector injector, ProgramOptions programOptions) {
    MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    return Arrays.asList(
      injector.getInstance(ZKClientService.class),
      injector.getInstance(KafkaClientService.class),
      injector.getInstance(BrokerService.class),
      metricsCollectionService,
      injector.getInstance(StreamCoordinatorClient.class),
      new ProgramRunnableResourceReporter(programOptions.getProgramId(), metricsCollectionService, context)
    );
  }

  @Override
  protected Map<String, String> getExtraSystemArguments() {
    Map<String, String> args = new HashMap<>();
    args.put(ProgramOptionConstants.INSTANCE_ID, Integer.toString(context.getInstanceId()));
    args.put(ProgramOptionConstants.INSTANCES, Integer.toString(context.getInstanceCount()));
    args.put(ProgramOptionConstants.TWILL_RUN_ID, context.getApplicationRunId().getId());
    args.put(ProgramOptionConstants.HOST, context.getHost().getCanonicalHostName());
    args.put(ProgramOptionConstants.CLUSTER_MODE, getClusterMode().name());
    return args;
  }

  @Override
  protected Module createModule(CConfiguration cConf, Configuration hConf,
                                ProgramOptions programOptions, ProgramRunId programRunId) {
    // Add bindings for stream. It is being used by various TwillRunnable, hence add it here
    // This also make the removal later easier since stream is deprecated
    return Modules.combine(super.createModule(cConf, hConf, programOptions, programRunId),
                           new DistributedProgramStreamModule());
  }

  @Override
  protected ClusterMode getClusterMode() {
    return ClusterMode.ON_PREMISE;
  }

  @Override
  protected Optional<ServiceAnnouncer> getServiceAnnouncer() {
    return Optional.ofNullable(context);
  }
}

