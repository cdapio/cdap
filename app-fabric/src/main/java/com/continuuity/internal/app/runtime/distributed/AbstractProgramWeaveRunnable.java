/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.guice.DataFabricFacadeModule;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramResourceReporter;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.KafkaConstants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.queue.QueueReaderFactory;
import com.continuuity.internal.app.queue.SingleQueue2Reader;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.internal.app.runtime.webapp.ExplodeJarHttpHandler;
import com.continuuity.internal.app.runtime.webapp.WebappHttpHandlerFactory;
import com.continuuity.internal.kafka.client.ZKKafkaClientService;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.ServiceAnnouncer;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Services;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.zookeeper.RetryStrategies;
import com.continuuity.weave.zookeeper.ZKClientService;
import com.continuuity.weave.zookeeper.ZKClientServices;
import com.continuuity.weave.zookeeper.ZKClients;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A {@link WeaveRunnable} for running a program through a {@link ProgramRunner}.
 *
 * @param <T> The {@link ProgramRunner} type.
 */
public abstract class AbstractProgramWeaveRunnable<T extends ProgramRunner> implements WeaveRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProgramWeaveRunnable.class);

  private String name;
  private String hConfName;
  private String cConfName;

  private Injector injector;
  private Program program;
  private ProgramOptions programOpts;
  private ProgramController controller;
  private Configuration hConf;
  private CConfiguration cConf;
  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private ProgramResourceReporter resourceReporter;
  private LogAppenderInitializer logAppenderInitializer;

  protected AbstractProgramWeaveRunnable(String name, String hConfName, String cConfName) {
    this.name = name;
    this.hConfName = hConfName;
    this.cConfName = cConfName;
  }

  protected abstract Class<T> getProgramClass();

  /**
   * Provides sets of configurations to put into the specification. Children classes can override
   * this method to provides custom configurations.
   */
  protected Map<String, String> getConfigs() {
    return ImmutableMap.of();
  }

  @Override
  public WeaveRunnableSpecification configure() {
    return WeaveRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.<String, String>builder()
                     .put("hConf", hConfName)
                     .put("cConf", cConfName)
                     .putAll(getConfigs())
                     .build())
      .build();
  }

  @Override
  public void initialize(WeaveContext context) {
    name = context.getSpecification().getName();
    Map<String, String> configs = context.getSpecification().getConfigs();

    LOG.info("Initialize runnable: " + name);
    try {
      CommandLine cmdLine = parseArgs(context.getApplicationArguments());

      // Loads configurations
      hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      UserGroupInformation.setConfiguration(hConf);

      cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());

      zkClientService =
        ZKClientServices.delegate(
          ZKClients.reWatchOnExpire(
            ZKClients.retryOnFailure(
              ZKClientService.Builder.of(cConf.get(Constants.Zookeeper.QUORUM))
                .setSessionTimeout(cConf.getInt(Constants.Zookeeper.CFG_SESSION_TIMEOUT_MILLIS,
                                                Constants.Zookeeper.DEFAULT_SESSION_TIMEOUT_MILLIS))
                .build(),
              RetryStrategies.fixDelay(2, TimeUnit.SECONDS)
            )
          )
        );
      String kafkaZKNamespace = cConf.get(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
      kafkaClientService = new ZKKafkaClientService(
        kafkaZKNamespace == null
          ? zkClientService
          : ZKClients.namespace(zkClientService, "/" + kafkaZKNamespace)
      );

      injector = Guice.createInjector(createModule(context, zkClientService, kafkaClientService));

      metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

      // Initialize log appender
      logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();

      try {
        program = injector.getInstance(ProgramFactory.class).create(cmdLine.getOptionValue(RunnableOptions.JAR));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      Arguments runtimeArguments
        = new Gson().fromJson(cmdLine.getOptionValue(RunnableOptions.RUNTIME_ARGS), BasicArguments.class);
      programOpts =  new SimpleProgramOptions(name, createProgramArguments(context, configs), runtimeArguments);
      resourceReporter = new ProgramRunnableResourceReporter(program, metricsCollectionService, context);

      LOG.info("Runnable initialized: " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
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
      LOG.info("Stopping runnable: {}", name);
      controller.stop().get();
      logAppenderInitializer.close();
    } catch (Exception e) {
      LOG.error("Fail to stop: {}", e, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void run() {
    LOG.info("Starting metrics service");
    Futures.getUnchecked(
      Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService, resourceReporter));

    LOG.info("Starting runnable: {}", name);
    controller = injector.getInstance(getProgramClass()).run(program, programOpts);
    final SettableFuture<ProgramController.State> state = SettableFuture.create();
    controller.addListener(new AbstractListener() {
      @Override
      public void stopped() {
        state.set(ProgramController.State.STOPPED);
      }

      @Override
      public void error(Throwable cause) {
        LOG.error("Program runner error out.", cause);
        state.set(ProgramController.State.ERROR);
      }
    }, MoreExecutors.sameThreadExecutor());

    LOG.info("Program stopped. State: {}", Futures.getUnchecked(state));
  }

  @Override
  public void destroy() {
    LOG.info("Releasing resources: {}", name);
    Futures.getUnchecked(
      Services.chainStop(resourceReporter, metricsCollectionService, kafkaClientService, zkClientService));
    LOG.info("Runnable stopped: {}", name);
  }

  private CommandLine parseArgs(String[] args) {
    Options opts = new Options()
      .addOption(createOption(RunnableOptions.JAR, "Program jar location"))
      .addOption(createOption(RunnableOptions.RUNTIME_ARGS, "Runtime arguments"));

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
   * Creates program arguments. It includes all configurations from the specification, excluding hConf and cConf.
   */
  private Arguments createProgramArguments(WeaveContext context, Map<String, String> configs) {
    Map<String, String> args = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.INSTANCE_ID, Integer.toString(context.getInstanceId()))
      .put(ProgramOptionConstants.INSTANCES, Integer.toString(context.getInstanceCount()))
      .put(ProgramOptionConstants.RUN_ID, context.getApplicationRunId().getId())
      .putAll(Maps.filterKeys(configs, Predicates.not(Predicates.in(ImmutableSet.of("hConf", "cConf")))))
      .build();

    return new BasicArguments(args);
  }

  // TODO(terence) make this works for different mode
  protected Module createModule(final WeaveContext context, ZKClientService zkClientService,
                                final KafkaClientService kafkaClientService) {
    return Modules.combine(new ConfigModule(cConf, hConf),
                           new IOModule(),
                           new MetricsClientRuntimeModule(kafkaClientService).getDistributedModules(),
                           new LocationRuntimeModule().getDistributedModules(),
                           new LoggingModules().getDistributedModules(),
                           new DataFabricModules(cConf, hConf).getDistributedModules(),
                           new AbstractModule() {
      @Override
      protected void configure() {
        bind(InetAddress.class).annotatedWith(Names.named(Constants.AppFabric.SERVER_ADDRESS))
                               .toInstance(context.getHost());
        // For program loading
        install(createProgramFactoryModule());

        // For Binding queue reader stuff (for flowlets)
        install(createFactoryModule(QueueReaderFactory.class,
                                    QueueReader.class,
                                    SingleQueue2Reader.class));

        // For binding DataSet transaction stuff
        install(new DataFabricFacadeModule());

        bind(ServiceAnnouncer.class).toInstance(new ServiceAnnouncer() {
          @Override
          public Cancellable announce(String serviceName, int port) {
            return context.announce(serviceName, port);
          }
        });

        // Create webapp http handler factory.
        install(new FactoryModuleBuilder().implement(HttpHandler.class, ExplodeJarHttpHandler.class)
                  .build(WebappHttpHandlerFactory.class));
      }
    });
  }

  private <T> Module createFactoryModule(final Class<?> factoryClass,
                                         final Class<T> sourceClass,
                                         final Class<? extends T> targetClass) {
    return new FactoryModuleBuilder()
      .implement(sourceClass, targetClass)
      .build(factoryClass);
  }


  private Module createProgramFactoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(LocationFactory.class)
          .annotatedWith(Names.named("program.location.factory"))
          .toInstance(new LocalLocationFactory(new File(System.getProperty("user.dir"))));
        bind(ProgramFactory.class).in(Scopes.SINGLETON);
        expose(ProgramFactory.class);
      }
    };
  }

  /**
   * A private factory for creating instance of Program.
   * It's needed so that we can inject different LocationFactory just for loading program.
   */
  private static final class ProgramFactory {

    private final LocationFactory locationFactory;

    @Inject
    ProgramFactory(@Named("program.location.factory") LocationFactory locationFactory) {
      this.locationFactory = locationFactory;
    }

    public Program create(String path) throws IOException {
      return Programs.create(locationFactory.create(path));
    }
  }
}

