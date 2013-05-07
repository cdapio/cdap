/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.guice.IOModule;
import com.continuuity.app.program.Program;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.api.io.SchemaGenerator;
import com.continuuity.internal.app.queue.QueueReaderFactory;
import com.continuuity.internal.app.queue.SingleQueueReader;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.continuuity.internal.app.runtime.SmartDataFabricFacade;
import com.continuuity.internal.app.runtime.flow.FlowletProgramRunner;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * A {@link WeaveRunnable} for running a {@link com.continuuity.api.flow.flowlet.Flowlet}.
 */
public final class FlowletWeaveRunnable implements WeaveRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletWeaveRunnable.class);

  private String name;
  private String hConfName;
  private String cConfName;

  private Injector injector;
  private Program program;
  private ProgramOptions programOpts;
  private ProgramController controller;
  private Configuration hConf;
  private CConfiguration cConf;

  public FlowletWeaveRunnable(String name, String hConfName, String cConfName) {
    this.name = name;
    this.hConfName = hConfName;
    this.cConfName = cConfName;
  }

  @Override
  public WeaveRunnableSpecification configure() {
    return WeaveRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.of(
        "hConf", hConfName,
        "cConf", cConfName
      ))
      .build();
  }

  @Override
  public void initialize(WeaveContext context) {
    name = context.getSpecification().getName();
    Map<String,String> configs = context.getSpecification().getConfigs();

    LOG.info("Initialize flowlet runnable: " + name);
    try {
      CommandLine cmdLine = parseArgs(context.getArguments());

      // Loads configurations
      hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());

      injector = Guice.createInjector(createModule());
      try {
        program = new Program(injector.getInstance(LocationFactory.class)
                                .create(cmdLine.getOptionValue(FlowletOptions.JAR)));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      programOpts = new com.continuuity.internal.app.runtime.flow.FlowletOptions(name,
                                       context.getInstanceId(),
                                       Integer.parseInt(cmdLine.getOptionValue(FlowletOptions.INSTANCES)),
                                       RunId.from(cmdLine.getOptionValue(FlowletOptions.RUN_ID)));

      LOG.info("Flowlet runnable initialized: " + name);
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw Throwables.propagate(t);
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    if (FlowletCommands.SUSPEND.equals(command)) {
      controller.suspend().get();
      return;
    }
    if (FlowletCommands.RESUME.equals(command)) {
      controller.resume().get();
      return;
    }
    if ("instances".equals(command.getCommand())) {
      int instances = Integer.parseInt(command.getOptions().get("count"));
      controller.command("instances", instances);
      return;
    }
    LOG.warn("Ignore unsupported command: " + command);
  }

  @Override
  public void stop() {
    try {
      LOG.info("Stopping flowlet runnable: " + name);
      controller.stop().get();
      LOG.info("Flowlet runnable stopped: " + name);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void run() {
    LOG.info("Starting flowlet runnable: " + name);
    controller = injector.getInstance(FlowletProgramRunner.class).run(program, programOpts);
    final SettableFuture<ProgramController.State> state = SettableFuture.create();
    controller.addListener(new AbstractListener() {
      @Override
      public void stopped() {
        state.set(ProgramController.State.STOPPED);
      }

      @Override
      public void error() {
        state.set(ProgramController.State.ERROR);
      }
    }, MoreExecutors.sameThreadExecutor());

    LOG.info("Flowlet program runner terminated. State: ", Futures.getUnchecked(state));
  }

  private CommandLine parseArgs(String[] args) {
    Options opts = new Options()
      .addOption(createOption(FlowletOptions.JAR, "Program jar location"))
      .addOption(createOption(FlowletOptions.INSTANCES, "Total number of flowlet instances"))
      .addOption(createOption(FlowletOptions.RUN_ID, "Run id of the Flow process."));

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

  // TODO(terence) make this works for different mode
  private Module createModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(LocationFactory.class).to(LocalLocationFactory.class).in(Scopes.SINGLETON);
        bind(SchemaGenerator.class).to(ReflectionSchemaGenerator.class).in(Scopes.SINGLETON);

        // For binding DataSet transaction stuff
        install(createFactoryModule(DataFabricFacadeFactory.class,
                                    DataFabricFacade.class,
                                    SmartDataFabricFacade.class));

        // For Binding queue stuff
        install(createFactoryModule(QueueReaderFactory.class, QueueReader.class, SingleQueueReader.class));

        // For datum decode.
        install(new IOModule());

        // Bind remote operation executor
        bind(OperationExecutor.class).to(RemoteOperationExecutor.class).in(Singleton.class);
        bind(CConfiguration.class).annotatedWith(Names.named("RemoteOperationExecutorConfig")).toInstance(cConf);
      }
    };
  }

  private <T> Module createFactoryModule(final Class<?> factoryClass,
                                         final Class<T> sourceClass,
                                         final Class<? extends T> targetClass) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        install(new FactoryModuleBuilder()
                  .implement(sourceClass, targetClass)
                  .build(factoryClass));
      }
    };
  }
}
