/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DistributedProgramContainerModule;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitors;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.appender.loader.LogAppenderLoaderService;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.Command;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.Constants;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.ProxySelector;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * A {@link TwillRunnable} for running a program through a {@link ProgramRunner}.
 *
 * @param <T> The {@link ProgramRunner} type.
 */
public abstract class AbstractProgramTwillRunnable<T extends ProgramRunner> implements TwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramTwillRunnable.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .registerTypeAdapter(org.apache.twill.internal.Arguments.class,
                         new org.apache.twill.internal.json.ArgumentsCodec())
    .create();

  protected String name;

  private LogAppenderInitializer logAppenderInitializer;
  private ProgramOptions programOptions;
  private Deque<Service> coreServices;
  private ProxySelector oldProxySelector;
  private T programRunner;
  private Program program;
  private ProgramRunId programRunId;
  private TwillContext context;
  private CompletableFuture<ProgramController> controllerFuture;
  private CompletableFuture<ProgramController.State> programCompletion;
  private long maxStopSeconds;

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
  public final void initialize(TwillContext context) {
    this.context = context;
    if (name == null) {
      name = context.getSpecification().getName();
    }

    LOG.info("Initializing runnable: " + name);

    try {
      doInitialize(new File(context.getApplicationArguments()[0]));
      LOG.info("Runnable initialized: {}", name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  /**
   * Prepares this instance to execute a program.
   *
   * @param programOptionFile a json file containing the serialized {@link ProgramOptions}
   * @throws Exception if failed to initialize
   */
  private void doInitialize(File programOptionFile) throws Exception {
    controllerFuture = new CompletableFuture<>();
    programCompletion = new CompletableFuture<>();

    // Setup process wide settings
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    System.setSecurityManager(new ProgramContainerSecurityManager(System.getSecurityManager()));
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();

    // Create the ProgramOptions
    programOptions = createProgramOptions(programOptionFile);
    programRunId = programOptions.getProgramId().run(ProgramRunners.getRunId(programOptions));

    Arguments systemArgs = programOptions.getArguments();
    LoggingContextAccessor.setLoggingContext(LoggingContextHelper.getLoggingContextWithRunId(programRunId,
                                                                                             systemArgs.asMap()));
    ClusterMode clusterMode = ProgramRunners.getClusterMode(programOptions);

    // Loads configurations
    Configuration hConf = new Configuration();
    if (clusterMode == ClusterMode.ON_PREMISE) {
      hConf.clear();
      hConf.addResource(new File(systemArgs.getOption(ProgramOptionConstants.HADOOP_CONF_FILE)).toURI().toURL());
    }
    UserGroupInformation.setConfiguration(hConf);

    CConfiguration cConf = CConfiguration.create();
    cConf.clear();
    cConf.addResource(new File(systemArgs.getOption(ProgramOptionConstants.CDAP_CONF_FILE)).toURI().toURL());

    maxStopSeconds = cConf.getLong(io.cdap.cdap.common.conf.Constants.AppFabric.PROGRAM_MAX_STOP_SECONDS);

    Injector injector = Guice.createInjector(createModule(cConf, hConf, programOptions, programRunId));

    // Setup the proxy selector for in active monitoring mode
    oldProxySelector = ProxySelector.getDefault();
    if (clusterMode == ClusterMode.ISOLATED) {
      RuntimeMonitors.setupMonitoring(injector, programOptions);
    }

    logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);

    // Create list of core services. They'll will be started in the run method and shutdown when the run
    // method completed
    coreServices = createCoreServices(injector, programOptions);

    // Create the ProgramRunner
    programRunner = createProgramRunner(injector);

    // Create the Program instance
    Location programJarLocation =
      Locations.toLocation(new File(systemArgs.getOption(ProgramOptionConstants.PROGRAM_JAR)));
    ApplicationSpecification appSpec = readJsonFile(
      new File(systemArgs.getOption(ProgramOptionConstants.APP_SPEC_FILE)), ApplicationSpecification.class);
    program = Programs.create(cConf, programRunner,
                              new ProgramDescriptor(programOptions.getProgramId(), appSpec), programJarLocation,
                              new File(systemArgs.getOption(ProgramOptionConstants.EXPANDED_PROGRAM_JAR)));
  }

  @Override
  public void run() {
    startCoreServices();

    try {
      LOG.info("Starting program run {}", programRunId);

      // Start the program.
      ProgramController controller = programRunner.run(program, programOptions);
      controller.addListener(new AbstractListener() {
        @Override
        public void alive() {
          controllerFuture.complete(controller);
        }

        @Override
        public void completed() {
          controllerFuture.complete(controller);
          programCompletion.complete(ProgramController.State.COMPLETED);
        }

        @Override
        public void killed() {
          controllerFuture.complete(controller);
          programCompletion.complete(ProgramController.State.KILLED);
        }

        @Override
        public void error(Throwable cause) {
          controllerFuture.complete(controller);
          programCompletion.completeExceptionally(cause);
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      // Block on the completion
      programCompletion.get();
    } catch (InterruptedException e) {
      LOG.warn("Program {} interrupted.", name, e);
    } catch (ExecutionException e) {
      LOG.error("Program {} execution failed.", name, e);
      throw Throwables.propagate(Throwables.getRootCause(e));
    } finally {
      LOG.info("Program run {} completed. Releasing resources.", programRunId);

      // Close the Program and the ProgramRunner
      Closeables.closeQuietly(program);
      if (programRunner instanceof Closeable) {
        Closeables.closeQuietly((Closeable) programRunner);
      }

      stopCoreServices();
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    // need to make sure controller exists before handling the command
    if (ProgramCommands.SUSPEND.equals(command)) {
      controllerFuture.get().suspend().get();
      return;
    }
    if (ProgramCommands.RESUME.equals(command)) {
      controllerFuture.get().resume().get();
      return;
    }
    if (ProgramOptionConstants.INSTANCES.equals(command.getCommand())) {
      int instances = Integer.parseInt(command.getOptions().get("count"));
      controllerFuture.get().command(ProgramOptionConstants.INSTANCES, instances).get();
      return;
    }
    LOG.warn("Ignore unsupported command: " + command);
  }

  @Override
  public void stop() {
    try {
      // If the program is not completed, get the controller and call stop
      CompletableFuture<ProgramController.State> programCompletion = this.programCompletion;

      // If there is no program completion future or it is already done, simply return as there is nothing to stop.
      if (programCompletion == null || programCompletion.isDone()) {
        return;
      }

      // Don't block forever to get the controller. The controller future might be empty if there is
      // systematic failure such that program runner is not reacting correctly
      ProgramController controller = controllerFuture.get(5, TimeUnit.SECONDS);

      LOG.info("Stopping runnable: {}.", name);

      // Give some time for the program to stop
      controller.stop().get(maxStopSeconds == 0L ? Constants.APPLICATION_MAX_STOP_SECONDS : maxStopSeconds,
                            TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void destroy() {
    ProxySelector.setDefault(oldProxySelector);
  }

  /**
   * Returns a set of extra system arguments that will be available through the {@link ProgramOptions#getArguments()}
   * for the program execution.
   */
  private Map<String, String> getExtraSystemArguments() {
    Map<String, String> args = new HashMap<>();
    args.put(ProgramOptionConstants.INSTANCE_ID, context == null ? "0" : Integer.toString(context.getInstanceId()));
    args.put(ProgramOptionConstants.INSTANCES, context == null ? "1" : Integer.toString(context.getInstanceCount()));
    args.put(ProgramOptionConstants.TWILL_RUN_ID, context.getApplicationRunId().getId());
    args.put(ProgramOptionConstants.HOST, context.getHost().getCanonicalHostName());
    return args;
  }

  /**
   * Creates a Guice {@link Module} that will be used to create the Guice {@link Injector} used for the
   * program execution.
   *
   * @param cConf the CDAP configuration
   * @param hConf the Hadoop configuration
   * @param programOptions the {@link ProgramOptions} for the program execution
   * @param programRunId the {@link ProgramRunId} for this program run.
   * @return a guice {@link Module}.
   */
  protected Module createModule(CConfiguration cConf, Configuration hConf,
                                ProgramOptions programOptions, ProgramRunId programRunId) {
    return new DistributedProgramContainerModule(cConf, hConf, programRunId, programOptions, getServiceAnnouncer());
  }

  /**
   * Returns the {@link ServiceAnnouncer} to use for the program execution or {@code null} if service announcement
   * from program is not supported.
   */
  @Nullable
  protected ServiceAnnouncer getServiceAnnouncer() {
    return context;
  }

  /**
   * Creates a {@link ProgramRunner} for the running the program in this class.
   */
  protected T createProgramRunner(Injector injector) {
    Type type = TypeToken.of(getClass()).getSupertype(AbstractProgramTwillRunnable.class).getType();
    // Must be ParameterizedType
    Preconditions.checkState(type instanceof ParameterizedType,
                             "Invalid class %s. Expected to be a ParameterizedType.", getClass());

    Type programRunnerType = ((ParameterizedType) type).getActualTypeArguments()[0];
    // the ProgramRunnerType must be a Class
    Preconditions.checkState(programRunnerType instanceof Class,
                             "ProgramRunner type is not a class: %s", programRunnerType);

    @SuppressWarnings("unchecked")
    Class<T> programRunnerClass = (Class<T>) programRunnerType;
    return injector.getInstance(programRunnerClass);
  }

  /**
   * Creates a {@link ProgramOptions} by deserializing the given json file.
   */
  private ProgramOptions createProgramOptions(File programOptionsFile) throws IOException {
    ProgramOptions original = readJsonFile(programOptionsFile, ProgramOptions.class);

    // Overwrite them with environmental information
    Map<String, String> arguments = new HashMap<>(original.getArguments().asMap());
    arguments.putAll(getExtraSystemArguments());

    // Use the name passed in by the constructor as the program name to construct the ProgramId
    return new SimpleProgramOptions(original.getProgramId(), new BasicArguments(arguments),
                                    original.getUserArguments(), original.isDebug());
  }

  /**
   * Reads the content of the given file and decode it as json.
   */
  private <U> U readJsonFile(File file, Class<U> type) throws IOException {
    try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
      return GSON.fromJson(reader, type);
    }
  }

  /**
   * Returns a list of {@link Service} to start before the program execution and shutdown when program completed.
   */
  private Deque<Service> createCoreServices(Injector injector, ProgramOptions programOptions) {
    Deque<Service> services = new LinkedList<>();

    MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    services.add(metricsCollectionService);
    services.add(injector.getInstance(LogAppenderLoaderService.class));

    if (ProgramRunners.getClusterMode(programOptions) == ClusterMode.ON_PREMISE) {
      addOnPremiseServices(injector, programOptions, metricsCollectionService, services);
    }

    return services;
  }

  private void addOnPremiseServices(Injector injector, ProgramOptions programOptions,
                                    MetricsCollectionService metricsCollectionService, Collection<Service> services) {
    services.add(injector.getInstance(ZKClientService.class));
    services.add(injector.getInstance(KafkaClientService.class));
    services.add(injector.getInstance(BrokerService.class));
    services.add(new ProgramRunnableResourceReporter(programOptions.getProgramId(), metricsCollectionService, context));
  }

  private void startCoreServices() {
    // Initialize log appender
    logAppenderInitializer.initialize();
    SystemArguments.setLogLevel(programOptions.getUserArguments(), logAppenderInitializer);

    try {
      // Starts the core services
      for (Service service : coreServices) {
        service.startAndWait();
      }
    } catch (Exception e) {
      logAppenderInitializer.close();
      throw e;
    }
  }

  private void stopCoreServices() {
    // Stop all services. Reverse the order.
    for (Service service : (Iterable<Service>) coreServices::descendingIterator) {
      try {
        service.stopAndWait();
      } catch (Exception e) {
        LOG.warn("Exception raised when stopping service {} during program termination.", service, e);
      }
    }
    logAppenderInitializer.close();
  }
}

