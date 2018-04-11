/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.guice.DistributedProgramContainerModule;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.logging.common.UncaughtExceptionHandler;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;

/**
 * @param <T> type of the {@link ProgramRunner} to run a program.
 */
public abstract class DistributedProgramContainer<T extends ProgramRunner> {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramContainer.class);
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
    .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
    .create();

  private LogAppenderInitializer logAppenderInitializer;
  private ProgramOptions programOptions;
  private List<Service> coreServices = Collections.emptyList();
  private T programRunner;
  private Program program;
  private ProgramRunId programRunId;

  /**
   * Initialize the container and prepare it to run the necessary services and program. This method will
   * do not start any services or program.
   *
   * @param args container arguments
   * @throws Exception if there is failure during initialization
   */
  public final void initialize(String[] args) throws Exception {
    // Setup process wide settings
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    System.setSecurityManager(new ProgramContainerSecurityManager(System.getSecurityManager()));
    SLF4JBridgeHandler.install();

    // Create the ProgramOptions
    programOptions = createProgramOptions(new File(args[0]));
    programRunId = programOptions.getProgramId().run(ProgramRunners.getRunId(programOptions));

    Arguments systemArgs = programOptions.getArguments();

    // Loads configurations
    Configuration hConf = new Configuration();
    hConf.clear();
    hConf.addResource(systemArgs.getOption(ProgramOptionConstants.HADOOP_CONF_FILE));

    UserGroupInformation.setConfiguration(hConf);

    CConfiguration cConf = CConfiguration.create();
    cConf.clear();
    cConf.addResource(systemArgs.getOption(ProgramOptionConstants.CDAP_CONF_FILE));

    Injector injector = Guice.createInjector(createModule(cConf, hConf, programOptions, programRunId));

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

  /**
   * Executes the program prepared in the {@link #initialize(String[])} phase. Core services will be started
   * prior to the execution of the program.
   *
   * @return a {@link ProgramController} for the running program.
   */
  protected final ProgramController executeProgram() {
    // Initialize log appender
    logAppenderInitializer.initialize();

    try {
      // Starts the core services
      for (Service service : coreServices) {
        try {
          service.startAndWait();
        } catch (Exception e) {
          // Log here so that we can log the service name.
          LOG.error("Failed to start service {}.", service, e);
          throw e;
        }
      }
    } catch (Exception e) {
      logAppenderInitializer.close();
      throw e;
    }

    LOG.info("Starting program run {}", programRunId);

    // Start the program.
    ProgramController controller = programRunner.run(program, programOptions);
    addListener(controller);

    return controller;
  }

  private void addListener(ProgramController controller) {
    CountDownLatch block = new CountDownLatch(1);
    controller.addListener(new AbstractListener() {
      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        switch (currentState) {
          case ALIVE:
            alive();
            break;
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
      public void alive() {
        block.countDown();
      }

      @Override
      public void completed() {
        finished();
      }

      @Override
      public void killed() {
        finished();
      }

      @Override
      public void error(Throwable cause) {
        finished();
      }

      private void finished() {
        block.countDown();

        LOG.info("Program run {} completed. Releasing resources.", programRunId);

        // Close the Program and the ProgramRunner
        Closeables.closeQuietly(program);
        if (programRunner instanceof Closeable) {
          Closeables.closeQuietly((Closeable) programRunner);
        }

        // Stop all services
        for (Service service : coreServices) {
          try {
            service.stopAndWait();
          } catch (Exception e) {
            LOG.warn("Exception raised when stopping service {} during program termination.", service, e);
          }
        }
        logAppenderInitializer.close();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Block until the controller is alive or finished. The blocking should be short.
    Uninterruptibles.awaitUninterruptibly(block);
  }

  protected abstract ClusterMode getClusterMode();

  protected abstract Optional<ServiceAnnouncer> getServiceAnnouncer();

  protected abstract List<Service> createCoreServices(Injector injector, ProgramOptions programOptions);

  protected Module createModule(CConfiguration cConf, Configuration hConf,
                                ProgramOptions programOptions, ProgramRunId programRunId) {
    Arguments systemArgs = programOptions.getArguments();

    DistributedProgramContainerModule.Builder builder = DistributedProgramContainerModule.builder(
      cConf, hConf, programRunId, systemArgs.getOption(ProgramOptionConstants.INSTANCE_ID));

    // If kerberos is enabled we expect the principal to be provided in the program options.
    // If kerberos is disabled this will be null
    Optional.ofNullable(systemArgs.getOption(ProgramOptionConstants.PRINCIPAL)).ifPresent(builder::setPrincipal);
    getServiceAnnouncer().ifPresent(builder::setServiceAnnouncer);

    builder.setClusterMode(getClusterMode());

    return builder.build();
  }


  protected Map<String, String> getExtraSystemArguments() {
    Map<String, String> args = new HashMap<>();
    args.put(ProgramOptionConstants.INSTANCE_ID, "0");
    args.put(ProgramOptionConstants.INSTANCES, "1");
    try {
      args.put(ProgramOptionConstants.HOST, InetAddress.getLocalHost().getCanonicalHostName());
    } catch (UnknownHostException e) {
      // OK if can't determine the host. Sub-class can override this otherwise.
    }
    return args;
  }

  /**
   * Resolve the scope of the user arguments from the {@link Arguments}
   */
  protected Arguments resolveScope(Arguments args) {
    return args;
  }

  /**
   * Creates a {@link ProgramRunner} for the running the program in this class.
   */
  protected T createProgramRunner(Injector injector) {
    Type type = TypeToken.of(getClass()).getSupertype(DistributedProgramContainer.class).getType();
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


  private ProgramOptions createProgramOptions(File programOptionsFile) throws IOException {
    ProgramOptions original = readJsonFile(programOptionsFile, ProgramOptions.class);

    // Overwrite them with environmental information
    Map<String, String> arguments = new HashMap<>(original.getArguments().asMap());
    arguments.putAll(getExtraSystemArguments());

    // Use the name passed in by the constructor as the program name to construct the ProgramId
    return new SimpleProgramOptions(original.getProgramId(), new BasicArguments(arguments),
                                    resolveScope(original.getUserArguments()), original.isDebug());
  }

  /**
   * Reads the content of the given file and decode it as json.
   */
  private <U> U readJsonFile(File file, Class<U> type) throws IOException {
    try (Reader reader = Files.newReader(file, Charsets.UTF_8)) {
      return GSON.fromJson(reader, type);
    }
  }
}
