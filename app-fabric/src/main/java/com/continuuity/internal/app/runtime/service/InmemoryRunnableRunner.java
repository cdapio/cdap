package com.continuuity.internal.app.runtime.service;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.metrics.ServiceRunnableMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.lang.InstantiatorFactory;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.internal.app.runtime.MetricsFieldSetter;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.flow.FlowletProgramRunner;
import com.continuuity.internal.lang.Reflections;
import com.continuuity.logging.context.ServiceRunnableLoggingContext;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.internal.BasicTwillContext;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Runs runnable-service in single-node
 */
public class InmemoryRunnableRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlowletProgramRunner.class);

  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public InmemoryRunnableRunner(MetricsCollectionService metricsCollectionService) {
    this.metricsCollectionService = metricsCollectionService;
  }


  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    BasicTwillContext twillContext = null;
    try {
      // Extract and verify parameters
      String runnableName = options.getName();

      int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
      Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

      int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
      Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

      String runIdOption = options.getArguments().getOption(ProgramOptionConstants.RUN_ID);
      Preconditions.checkNotNull(runIdOption, "Missing runId");
      RunId runId = RunIds.fromString(runIdOption);

      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      Type processorType = program.getType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.SERVICE, "Only FLOW process type is supported.");

      String processorName = program.getName();
      Preconditions.checkNotNull(processorName, "Missing processor name.");

      ServiceSpecification serviceSpec = appSpec.getServices().get(processorName);
      RuntimeSpecification runnableSpec = serviceSpec.getRunnables().get(runnableName);
      Preconditions.checkNotNull(runnableSpec, "Definition missing for Runnable \"%s\"", runnableName);

      Class<?> clz = null;
      clz = Class.forName(runnableSpec.getRunnableSpecification().getClassName(), true,
                          program.getMainClass().getClassLoader());

      Preconditions.checkArgument(TwillRunnable.class.isAssignableFrom(clz), "%s is not a TwillRunnable.", clz);

      Class<? extends TwillRunnable> runnableClass = (Class<? extends TwillRunnable>) clz;

      // todo we might have to generate runId for runnable before
      RunId twillRunId = RunIds.generate();
      // have to fill in the null values
      twillContext = new BasicTwillContext(twillRunId, runId, InetAddress.getLocalHost(), null, null,
                                           runnableSpec.getRunnableSpecification(), instanceId, null, null,
                                           instanceCount, runnableSpec.getResourceSpecification().getMemorySize(),
                                           runnableSpec.getResourceSpecification().getVirtualCores());

      TwillRunnable runnable = new InstantiatorFactory(false).get(TypeToken.of(runnableClass)).create();
      TypeToken<? extends TwillRunnable> runnableType = TypeToken.of(runnableClass);
      InMemoryRunnableDriver driver = new InMemoryRunnableDriver(
        runnable, twillContext, new ServiceRunnableLoggingContext(
        program.getAccountId(), program.getApplicationId(), runId.getId(), twillRunId.getId()));

      Reflections.visit(runnable, TypeToken.of(runnable.getClass()),
                        new MetricsFieldSetter(new ServiceRunnableMetrics
                                                 (metricsCollectionService, program.getApplicationId(),
                                                  serviceSpec.getName(), runnableName)));

      InMemoryRunnableProgramController controller = new InMemoryRunnableProgramController
        (program.getName(), runnableName, twillContext, driver);
      runnable.configure();
      runnable.initialize(twillContext);
      LOG.info("Starting flowlet: {}", twillContext);
      driver.start();
      LOG.info("Flowlet started: {}", twillContext);

      return controller;

    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    // have to return ProgramController for the runnable
    return null;
  }
}

