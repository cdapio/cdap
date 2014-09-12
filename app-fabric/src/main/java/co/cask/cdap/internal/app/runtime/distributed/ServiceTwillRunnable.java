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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.metrics.ServiceRunnableMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramResourceReporter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.services.HttpServiceTwillRunnable;
import co.cask.cdap.internal.app.services.ServiceWorkerTwillRunnable;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
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
import org.apache.twill.api.Command;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Services;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.RunIds;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Wrapper TwillRunnable around User Twill Runnable.
 */
public class ServiceTwillRunnable implements TwillRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceTwillRunnable.class);

  private String name;
  private String hConfName;
  private String cConfName;

  private Injector injector;
  private Program program;
  private ProgramOptions programOpts;
  private Configuration hConf;
  private CConfiguration cConf;
  private ZKClientService zkClientService;
  private KafkaClientService kafkaClientService;
  private MetricsCollectionService metricsCollectionService;
  private ProgramResourceReporter resourceReporter;
  private LogAppenderInitializer logAppenderInitializer;
  private TransactionSystemClient transactionSystemClient;
  private DiscoveryServiceClient discoveryServiceClient;
  private DatasetFramework datasetFramework;
  private TwillRunnable delegate;
  private String runnableName;

  protected ServiceTwillRunnable(String name, String hConfName, String cConfName) {
    this.name = name;
    this.hConfName = hConfName;
    this.cConfName = cConfName;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.<String, String>builder()
                     .put("hConf", hConfName)
                     .put("cConf", cConfName)
                     .build())
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
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

      injector = Guice.createInjector(createModule(context));

      zkClientService = injector.getInstance(ZKClientService.class);
      kafkaClientService = injector.getInstance(KafkaClientService.class);
      metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

      // Initialize log appender
      logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
      logAppenderInitializer.initialize();

      transactionSystemClient = injector.getInstance(TransactionSystemClient.class);
      datasetFramework = injector.getInstance(DatasetFramework.class);
      discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);

      try {
        program = injector.getInstance(ProgramFactory.class)
          .create(cmdLine.getOptionValue(RunnableOptions.JAR));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      Arguments runtimeArguments
        = new Gson().fromJson(cmdLine.getOptionValue(RunnableOptions.RUNTIME_ARGS), BasicArguments.class);
      programOpts = new SimpleProgramOptions(name, createProgramArguments(context, configs), runtimeArguments);
      resourceReporter = new ProgramRunnableResourceReporter(program, metricsCollectionService, context);

      // These services need to be starting before initializing the delegate since they are used in
      // AbstractContext's constructor to create datasets.
      Futures.getUnchecked(
        Services.chainStart(zkClientService, kafkaClientService, metricsCollectionService, resourceReporter));

      ApplicationSpecification appSpec = program.getSpecification();
      String processorName = program.getName();
      runnableName = programOpts.getName();

      Arguments arguments = programOpts.getArguments();
      RunId runId = arguments.hasOption(ProgramOptionConstants.RUN_ID)
        ? RunIds.fromString(arguments.getOption(ProgramOptionConstants.RUN_ID))
        : RunIds.generate();

      ServiceSpecification serviceSpec = appSpec.getServices().get(processorName);
      final RuntimeSpecification runtimeSpec = serviceSpec.getRunnables().get(runnableName);
      String className = runtimeSpec.getRunnableSpecification().getClassName();
      LOG.info("Getting class : {}", program.getMainClass().getName());
      Class<?> clz = Class.forName(className, true, program.getClassLoader());
      Preconditions.checkArgument(TwillRunnable.class.isAssignableFrom(clz), "%s is not a TwillRunnable.", clz);

      if (clz.isAssignableFrom(HttpServiceTwillRunnable.class)) {
        // Special case for running http services since we need to instantiate the http service
        // using the program classloader.
        delegate = new HttpServiceTwillRunnable(program, runId, cConf, runnableName, metricsCollectionService,
                                                discoveryServiceClient, datasetFramework,
                                                transactionSystemClient);
      } else if (clz.isAssignableFrom(ServiceWorkerTwillRunnable.class)) {
        delegate = new ServiceWorkerTwillRunnable(program, runId, runnableName, program.getClassLoader(), cConf,
                                                  metricsCollectionService, datasetFramework,
                                                  transactionSystemClient,
                                                  discoveryServiceClient);
      } else {
        delegate = (TwillRunnable) new InstantiatorFactory(false).get(TypeToken.of(clz)).create();
      }

      Reflections.visit(delegate, TypeToken.of(delegate.getClass()),
                        new MetricsFieldSetter(new ServiceRunnableMetrics(metricsCollectionService,
                                                                          program.getApplicationId(),
                                                                          program.getName(), runnableName,
                                                                          context.getInstanceId())),
                        new PropertyFieldSetter(runtimeSpec.getRunnableSpecification().getConfigs()));

      final String[] argArray = RuntimeArguments.toPosixArray(programOpts.getUserArguments());
      LoggingContextAccessor.setLoggingContext(new UserServiceLoggingContext(
        program.getAccountId(), program.getApplicationId(), program.getName(), runnableName));
      delegate.initialize(new ForwardingTwillContext(context) {
        @Override
        public String[] getApplicationArguments() {
          return argArray;
        }

        @Override
        public TwillRunnableSpecification getSpecification() {
          return runtimeSpec.getRunnableSpecification();
        }

        @Override
        public Cancellable announce(String runnable, int port) {
          String serviceContextPath = String.format("%s.%s.%s.%s", ProgramType.SERVICE.name().toLowerCase(),
                                               program.getAccountId(), program.getApplicationId(), program.getName());
          // Currently ignoring the runnable's name (the param passed into announce), and simply announcing by the name
          // of the service it belongs to (reasoning: the primary runnable of a service will be identified by the
          // service's name, and the other runnables within the service are not discoverable externally).
          return super.announce(serviceContextPath, port);
        }
      });

      LOG.info("Runnable initialized: " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    try {
      delegate.handleCommand(command);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void stop() {
    try {
      LOG.info("Stopping runnable: {}", name);
      delegate.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop: {}", e, e);
      throw Throwables.propagate(e);
    } finally {
      logAppenderInitializer.close();
    }
  }

  @Override
  public void run() {
    LOG.info("Starting runnable: {}", name);
    try {
      delegate.run();
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void destroy() {
    LOG.info("Releasing resources: {}", name);
    try {
      delegate.destroy();
    } finally {
      Futures.getUnchecked(
        Services.chainStop(resourceReporter, metricsCollectionService, kafkaClientService, zkClientService));
    }
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
  private Arguments createProgramArguments(TwillContext context, Map<String, String> configs) {
    Map<String, String> args = ImmutableMap.<String, String>builder()
      .put(ProgramOptionConstants.INSTANCE_ID, Integer.toString(context.getInstanceId()))
      .put(ProgramOptionConstants.INSTANCES, Integer.toString(context.getInstanceCount()))
      .put(ProgramOptionConstants.RUN_ID, context.getApplicationRunId().getId())
      .putAll(Maps.filterKeys(configs, Predicates.not(Predicates.in(ImmutableSet.of("hConf", "cConf")))))
      .build();

    return new BasicArguments(args);
  }

  protected Module createModule(final TwillContext context) {
    return Modules.combine(
      new ConfigModule(cConf, hConf),
      new IOModule(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new AuthModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new LocationRuntimeModule().getDistributedModules(),
      new LoggingModules().getDistributedModules(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataFabricModules().getDistributedModules(),
      new DataSetsModules().getDistributedModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // For program loading
          install(createProgramFactoryModule());

        }
      }
    );
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
      Location location = locationFactory.create(path);
      return Programs.createWithUnpack(location, Files.createTempDir());
    }
  }
}
