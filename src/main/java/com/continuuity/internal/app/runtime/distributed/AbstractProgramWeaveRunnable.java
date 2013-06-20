/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.internal.app.queue.QueueReaderFactory;
import com.continuuity.internal.app.queue.SingleQueueReader;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.continuuity.internal.app.runtime.SmartDataFabricFacade;
import com.continuuity.weave.api.Command;
import com.continuuity.weave.api.ServiceAnnouncer;
import com.continuuity.weave.api.WeaveContext;
import com.continuuity.weave.api.WeaveRunnable;
import com.continuuity.weave.api.WeaveRunnableSpecification;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
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

  protected AbstractProgramWeaveRunnable(String name, String hConfName, String cConfName) {
    this.name = name;
    this.hConfName = hConfName;
    this.cConfName = cConfName;
  }

  protected abstract Class<T> getProgramClass();

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
    Map<String, String> configs = context.getSpecification().getConfigs();

    LOG.info("Initialize runnable: " + name);
    try {
      CommandLine cmdLine = parseArgs(context.getArguments());

      // Loads configurations
      hConf = new Configuration();
      hConf.clear();
      hConf.addResource(new File(configs.get("hConf")).toURI().toURL());

      cConf = CConfiguration.create();
      cConf.clear();
      cConf.addResource(new File(configs.get("cConf")).toURI().toURL());

      OverlordMetricsReporter.enable(1, TimeUnit.SECONDS, cConf);

      injector = Guice.createInjector(createModule(context));
      try {
        program = new Program(injector.getInstance(LocationFactory.class)
                                .create(cmdLine.getOptionValue(RunnableOptions.JAR)));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      //
      Arguments runtimeArguments
        = new Gson().fromJson(cmdLine.getOptionValue(RunnableOptions.RUNTIME_ARGS), BasicArguments.class);
      programOpts =  new SimpleProgramOptions(name,
                                              new BasicArguments(ImmutableMap.of(
                                                "instanceId", Integer.toString(context.getInstanceId()),
                                                "instances", Integer.toString(context.getInstanceCount()),
                                                "runId", context.getApplicationRunId().getId())),
                                              runtimeArguments);

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
      LOG.info("Stopping runnable: " + name);
      controller.stop().get();
      LOG.info("Runnable stopped: " + name);
    } catch (Exception e) {
      LOG.error("Fail to stop. {}", e, e);
      throw Throwables.propagate(e);
    } finally {
      OverlordMetricsReporter.disable();
    }
  }

  @Override
  public void run() {
    LOG.info("Starting runnable: " + name);
    controller = injector.getInstance(getProgramClass()).run(program, programOpts);
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

    LOG.info("Program runner terminated. State: ", Futures.getUnchecked(state));
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

  // TODO(terence) make this works for different mode
  private Module createModule(final WeaveContext context) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(InetAddress.class).annotatedWith(Names.named("config.hostname")).toInstance(context.getHost());

        bind(LocationFactory.class).toInstance(new LocalLocationFactory(new File(System.getProperty("user.dir"))));

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

        bind(ServiceAnnouncer.class).toInstance(new ServiceAnnouncer() {
          @Override
          public Cancellable announce(String serviceName, int port) {
            return context.announce(serviceName, port);
          }
        });
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

