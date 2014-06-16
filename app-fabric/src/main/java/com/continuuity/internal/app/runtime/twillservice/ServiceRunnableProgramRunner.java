package com.continuuity.internal.app.runtime.twillservice;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.internal.app.runtime.MetricsFieldSetter;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.lang.Reflections;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Program runner for Services. Metrics and Logging contexts will be set before running the services.
 */
public class ServiceRunnableProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceRunnableProgramRunner.class);

  private final MetricsCollectionService metricsCollectionService;
  private final TwillContext twillContext;

  @Inject
  public ServiceRunnableProgramRunner(MetricsCollectionService metricsCollectionService, TwillContext twillContext) {
    this.metricsCollectionService = metricsCollectionService;
    this.twillContext = twillContext;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  void setLogWriter(LogWriter logWriter) {
    CAppender.logWriter = logWriter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    try {
      // Extract and verify parameters
      String runnableName = options.getName();

      ExecutorService executorService = Executors.newSingleThreadExecutor();

      int instanceId = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCE_ID, "-1"));
      Preconditions.checkArgument(instanceId >= 0, "Missing instance Id");

      int instanceCount = Integer.parseInt(options.getArguments().getOption(ProgramOptionConstants.INSTANCES, "0"));
      Preconditions.checkArgument(instanceCount > 0, "Invalid or missing instance count");

      String runIdOption = options.getArguments().getOption(ProgramOptionConstants.RUN_ID);
      Preconditions.checkNotNull(runIdOption, "Missing runId");
      RunId runId = RunIds.fromString(runIdOption);

      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification");

      Type processorType = program.getType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.SERVICE, "Only SERVICE process type is supported.");

      String processorName = program.getName();
      Preconditions.checkNotNull(processorName, "Missing processor name.");

      ServiceSpecification serviceSpec = appSpec.getServices().get(processorName);
      RuntimeSpecification runtimeSpec = serviceSpec.getRunnables().get(runnableName);
      Preconditions.checkNotNull(runtimeSpec, "Runtime Spec missing for Runnable \"%s\"", runnableName);

      String className = runtimeSpec.getRunnableSpecification().getClassName();
      Class<?> clz = Class.forName(className, true, program.getMainClass().getClassLoader());
      Preconditions.checkArgument(TwillRunnable.class.isAssignableFrom(clz), "%s is not a TwillRunnable.", clz);

      TwillRunnable twillRunnable = (TwillRunnable) clz.newInstance();
      ServiceRunnableContext runnableContext = new ServiceRunnableContext(program, runnableName, instanceId,
                                                                          runId, instanceCount, runtimeSpec,
                                                                          metricsCollectionService);

      Reflections.visit(twillRunnable, TypeToken.of(twillRunnable.getClass()),
                        new MetricsFieldSetter(runnableContext.getMetrics()));

      ServiceRunnableProgramController controller = new ServiceRunnableProgramController(program.getName(),
                                                                                         runnableName, runnableContext,
                                                                                         twillRunnable);

      LOG.info("Starting runnable: {} Class Name : {}", runnableName, className);
      LoggingContextAccessor.setLoggingContext(runnableContext.getLoggingContext());
      twillRunnable.initialize(twillContext);
      executorService.submit(twillRunnable);
      LOG.info("Runnable started: {} Service Name : {}", runnableName, program.getName());

      return controller;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
