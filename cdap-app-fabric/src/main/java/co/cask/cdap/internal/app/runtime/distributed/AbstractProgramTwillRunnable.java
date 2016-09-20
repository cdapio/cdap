/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.guice.DistributedProgramRunnableModule;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramResourceReporter;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.authorization.AuthorizationEnforcementService;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.Services;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.security.Permission;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
    .create();

  private String name;
  private T programRunner;
  private Program program;
  private ProgramOptions programOpts;
  private ProgramController controller;
  private Configuration hConf;
  private CConfiguration cConf;
  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private StreamCoordinatorClient streamCoordinatorClient;
  private ProgramResourceReporter resourceReporter;
  private LogAppenderInitializer logAppenderInitializer;
  private CountDownLatch runlatch;
  private AuthorizationEnforcementService authEnforcementService;

  /**
   * Constructor.
   *
   * @param name Name of the TwillRunnable
   */
  protected AbstractProgramTwillRunnable(String name) {
    this.name = name;
  }

  /**
   * Provides sets of configurations to put into the specification. Children classes can override
   * this method to provides custom configurations.
   */
  protected Map<String, String> getConfigs() {
    return ImmutableMap.of();
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(getConfigs())
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    System.setSecurityManager(new RunnableSecurityManager(System.getSecurityManager()));

    runlatch = new CountDownLatch(1);
    name = context.getSpecification().getName();
    LOG.info("Initializing runnable: " + name);
    try {
      CommandLine cmdLine = parseArgs(context.getApplicationArguments());

      // Loads configurations
      hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(cmdLine.getOptionValue(RunnableOptions.HADOOP_CONF_FILE)).toURI().toURL());

      UserGroupInformation.setConfiguration(hConf);

      cConf = CConfiguration.create(new File(cmdLine.getOptionValue(RunnableOptions.CDAP_CONF_FILE)));

      Injector injector = Guice.createInjector(createModule(context));

      zkClientService = injector.getInstance(ZKClientService.class);
      kafkaClientService = injector.getInstance(KafkaClientService.class);
      metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
      streamCoordinatorClient = injector.getInstance(StreamCoordinatorClient.class);

      // Initialize log appender
      logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();

      // Create the ProgramRunner
      programRunner = createProgramRunner(injector);

      try {
        Location programJarLocation = Locations.toLocation(new File(cmdLine.getOptionValue(RunnableOptions.JAR)));
        ProgramId programId = GSON.fromJson(cmdLine.getOptionValue(RunnableOptions.PROGRAM_ID), ProgramId.class);
        ApplicationSpecification appSpec = readAppSpec(new File(cmdLine.getOptionValue(RunnableOptions.APP_SPEC_FILE)));

        program = Programs.create(cConf, programRunner, new ProgramDescriptor(programId, appSpec),
                                  programJarLocation, BundleJarUtil.unJar(programJarLocation, Files.createTempDir()));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      programOpts = createProgramOptions(cmdLine, context, context.getSpecification().getConfigs());
      resourceReporter = new ProgramRunnableResourceReporter(program.getId(),
                                                             metricsCollectionService, context);

      authEnforcementService = injector.getInstance(AuthorizationEnforcementService.class);
      LOG.info("Runnable initialized: {}", name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    // need to make sure controller exists before handling the command
    runlatch.await();
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

  /**
   * Returns {@code true} if service error is propagated as error for this controller state.
   * If this method returns {@code false}, service error is just getting logged and state will just change
   * to {@link ProgramController.State#KILLED}. This method returns {@code true} by default.
   */
  protected boolean propagateServiceError() {
    return true;
  }

  /**
   * Creates a {@link ProgramRunner} for the running the program in this {@link TwillRunnable}.
   */
  protected T createProgramRunner(Injector injector) {
    Type type = TypeToken.of(getClass()).getSupertype(AbstractProgramTwillRunnable.class).getType();
    // Must be ParameterizedType
    Preconditions.checkState(type instanceof ParameterizedType,
                             "Invalid ProgramTwillRunnable class %s. Expected to be a ParameterizedType.", getClass());

    Type programRunnerType = ((ParameterizedType) type).getActualTypeArguments()[0];
    // the ProgramRunnerType must be a Class
    Preconditions.checkState(programRunnerType instanceof Class,
                             "ProgramRunner type is not a class: %s", programRunnerType);

    @SuppressWarnings("unchecked")
    Class<T> programRunnerClass = (Class<T>) programRunnerType;
    return injector.getInstance(programRunnerClass);
  }

  @Override
  public void run() {
    Futures.getUnchecked(
      Services.chainStart(zkClientService, kafkaClientService,
                          metricsCollectionService, streamCoordinatorClient, resourceReporter, authEnforcementService));

    LOG.info("Starting runnable: {}", name);
    controller = programRunner.run(program, programOpts);
    final SettableFuture<ProgramController.State> state = SettableFuture.create();
    controller.addListener(new AbstractListener() {

      @Override
      public void alive() {
        runlatch.countDown();
      }

      @Override
      public void init(ProgramController.State currentState, @Nullable Throwable cause) {
        if (currentState == ProgramController.State.ALIVE) {
          alive();
        } else {
          super.init(currentState, cause);
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
        LOG.error("Program runner error out.", cause);
        state.setException(cause);
      }
    }, MoreExecutors.sameThreadExecutor());

    try {
      state.get();
      LOG.info("Program {} stopped.", name);
    } catch (InterruptedException e) {
      LOG.warn("Program {} interrupted.", name, e);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      LOG.error("Program {} execution failed.", name, e);
      if (propagateServiceError()) {
        throw Throwables.propagate(Throwables.getRootCause(e));
      }
    } finally {
      if (programRunner instanceof Closeable) {
        Closeables.closeQuietly((Closeable) programRunner);
      }
      // Always unblock the handleCommand method if it is not unblocked before (e.g if program failed to start).
      // The controller state will make sure the corresponding command will be handled correctly in the correct state.
      runlatch.countDown();
    }
  }

  @Override
  public void destroy() {
    LOG.info("Releasing resources: {}", name);
    try {
      if (program != null) {
        Closeables.closeQuietly(program);
      }
      Futures.getUnchecked(
        Services.chainStop(authEnforcementService, resourceReporter, streamCoordinatorClient,
                           metricsCollectionService, kafkaClientService, zkClientService));
      LOG.info("Runnable stopped: {}", name);
    } finally {
      if (logAppenderInitializer != null) {
        logAppenderInitializer.close();
      }
    }
  }

  private CommandLine parseArgs(String[] args) {
    Options opts = new Options()
      .addOption(createOption(RunnableOptions.JAR, "Program jar location"))
      .addOption(createOption(RunnableOptions.HADOOP_CONF_FILE, "Hadoop config file"))
      .addOption(createOption(RunnableOptions.CDAP_CONF_FILE, "CDAP config file"))
      .addOption(createOption(RunnableOptions.APP_SPEC_FILE, "Application specification file"))
      .addOption(createOption(RunnableOptions.PROGRAM_OPTIONS, "Program options"))
      .addOption(createOption(RunnableOptions.PROGRAM_ID, "Program ID"));

    try {
      return new PosixParser().parse(opts, args);
    } catch (ParseException e) {
      throw Throwables.propagate(e);
    }
  }

  private Option createOption(String opt, String desc) {
    Option option = new Option(opt, true, desc);
    option.setRequired(true);
    return option;
  }

  /**
   * Creates program options. It contains program and user arguments as passed form the distributed program runner.
   * Extra program arguments are inserted based on the environment information (e.g. host, instance id). Also all
   * configs available through the TwillRunnable configs are also available through program arguments.
   */
  private ProgramOptions createProgramOptions(CommandLine cmdLine, TwillContext context, Map<String, String> configs) {
    ProgramOptions original = GSON.fromJson(cmdLine.getOptionValue(RunnableOptions.PROGRAM_OPTIONS),
                                            ProgramOptions.class);

    // Overwrite them with environmental information
    Map<String, String> arguments = Maps.newHashMap(original.getArguments().asMap());
    arguments.put(ProgramOptionConstants.INSTANCE_ID, Integer.toString(context.getInstanceId()));
    arguments.put(ProgramOptionConstants.INSTANCES, Integer.toString(context.getInstanceCount()));
    arguments.put(ProgramOptionConstants.TWILL_RUN_ID, context.getApplicationRunId().getId());
    arguments.put(ProgramOptionConstants.HOST, context.getHost().getCanonicalHostName());
    arguments.putAll(configs);

    return new SimpleProgramOptions(context.getSpecification().getName(), new BasicArguments(arguments),
                                    original.getUserArguments(), original.isDebug());
  }

  private ApplicationSpecification readAppSpec(File appSpecFile) throws IOException {
    try (Reader reader = Files.newReader(appSpecFile, Charsets.UTF_8)) {
      return GSON.fromJson(reader, ApplicationSpecification.class);
    }
  }

  protected Module createModule(TwillContext context) {
    return new DistributedProgramRunnableModule(cConf, hConf).createModule(context);
  }

  /**
   * A {@link SecurityManager} used by the runnable container. Currently it specifically disabling
   * Spark classes to call {@link System#exit(int)}, {@link Runtime#halt(int)}
   * and {@link System#setSecurityManager(SecurityManager)}.
   * This is to workaround modification done in HDP Spark (CDAP-3014).
   *
   * In general, we can use the security manager to restrict what system actions can be performed by program as well.
   */
  private static final class RunnableSecurityManager extends SecurityManager {

    private final SecurityManager delegate;

    private RunnableSecurityManager(@Nullable SecurityManager delegate) {
      this.delegate = delegate;
    }

    @Override
    public void checkPermission(Permission perm) {
      if ("setSecurityManager".equals(perm.getName()) && isFromSpark()) {
        throw new SecurityException("Set SecurityManager not allowed from Spark class: "
                                      + Arrays.toString(getClassContext()));
      }
      if (delegate != null) {
        delegate.checkPermission(perm);
      }
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      if ("setSecurityManager".equals(perm.getName()) && isFromSpark()) {
        throw new SecurityException("Set SecurityManager not allowed from Spark class: "
                                      + Arrays.toString(getClassContext()));
      }
      if (delegate != null) {
        delegate.checkPermission(perm, context);
      }
    }

    @Override
    public void checkExit(int status) {
      if (isFromSpark()) {
        throw new SecurityException("Exit not allowed from Spark class: " + Arrays.toString(getClassContext()));
      }
      if (delegate != null) {
        delegate.checkExit(status);
      }
    }

    /**
     * Returns true if the current class context has spark class.
     */
    private boolean isFromSpark() {
      for (Class c : getClassContext()) {
        if (c.getName().startsWith("org.apache.spark.")) {
          return true;
        }
      }
      return false;
    }
  }
}

