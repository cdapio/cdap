/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.app.runtime.AbstractProgramRuntimeService;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramResourceReporter;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.internal.app.program.TypeId;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.app.runtime.AbstractResourceReporter;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.flow.FlowUtils;
import com.continuuity.internal.app.runtime.service.SimpleRuntimeInfo;
import com.continuuity.weave.api.ResourceReport;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeaveRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public final class DistributedProgramRuntimeService extends AbstractProgramRuntimeService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProgramRuntimeService.class);

  // Pattern to split a Weave App name into [type].[accountId].[appName].[programName]
  private static final Pattern APP_NAME_PATTERN = Pattern.compile("^(\\S+)\\.(\\S+)\\.(\\S+)\\.(\\S+)$");

  private final WeaveRunner weaveRunner;

  // TODO (terence): Injection of Store and QueueAdmin is a hack for queue reconfiguration.
  // Need to remove it when FlowProgramRunner can runs inside Weave AM.
  private final Store store;
  private final QueueAdmin queueAdmin;
  private final ProgramResourceReporter resourceReporter;


  @Inject
  DistributedProgramRuntimeService(ProgramRunnerFactory programRunnerFactory, WeaveRunner weaveRunner,
                                   StoreFactory storeFactory, QueueAdmin queueAdmin,
                                   MetricsCollectionService metricsCollectionService) {
    super(programRunnerFactory);
    this.weaveRunner = weaveRunner;
    this.store = storeFactory.create();
    this.queueAdmin = queueAdmin;
    this.resourceReporter = new AppMasterResourceReporter(metricsCollectionService);
  }

  @Override
  public synchronized RuntimeInfo lookup(final RunId runId) {
    RuntimeInfo runtimeInfo = super.lookup(runId);
    if (runtimeInfo != null) {
      return runtimeInfo;
    }

    // Lookup all live applications and look for the one that matches runId
    String appName = null;
    WeaveController controller = null;
    for (WeaveRunner.LiveInfo liveInfo : weaveRunner.lookupLive()) {
      for (WeaveController c : liveInfo.getControllers()) {
        if (c.getRunId().equals(runId)) {
          appName = liveInfo.getApplicationName();
          controller = c;
          break;
        }
      }
      if (controller != null) {
        break;
      }
    }

    if (controller == null) {
      LOG.info("No running instance found for RunId {}", runId);
      // TODO (ENG-2623): How about mapreduce job?
      return null;
    }

    Matcher matcher = APP_NAME_PATTERN.matcher(appName);
    if (!matcher.matches()) {
      LOG.warn("Unrecognized application name pattern {}", appName);
      return null;
    }

    Type type = getType(matcher.group(1));
    if (type == null) {
      LOG.warn("Unrecognized program type {}", appName);
      return null;
    }
    Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), matcher.group(4));

    if (runtimeInfo != null) {
      runtimeInfo = createRuntimeInfo(type, programId, controller);
      updateRuntimeInfo(type, runId, runtimeInfo);
      return runtimeInfo;
    } else {
      LOG.warn("Unable to find program {} {}", type, programId);
      return null;
    }
  }

  @Override
  public synchronized Map<RunId, RuntimeInfo> list(Type type) {
    Map<RunId, RuntimeInfo> result = Maps.newHashMap();
    result.putAll(super.list(type));

    // Goes through all live application, filter out the one that match the given type.
    for (WeaveRunner.LiveInfo liveInfo : weaveRunner.lookupLive()) {
      String appName = liveInfo.getApplicationName();
      Matcher matcher = APP_NAME_PATTERN.matcher(appName);
      if (!matcher.matches()) {
        continue;
      }
      Type appType = getType(matcher.group(1));
      if (appType != type) {
        continue;
      }

      for (WeaveController controller : liveInfo.getControllers()) {
        RunId runId = controller.getRunId();
        if (result.containsKey(runId)) {
          continue;
        }

        Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), matcher.group(4));
        RuntimeInfo runtimeInfo = createRuntimeInfo(type, programId, controller);
        if (runtimeInfo != null) {
          result.put(runId, runtimeInfo);
          updateRuntimeInfo(type, runId, runtimeInfo);
        } else {
          LOG.warn("Unable to find program {} {}", type, programId);
        }
      }
    }
    return ImmutableMap.copyOf(result);
  }

  private RuntimeInfo createRuntimeInfo(Type type, Id.Program programId, WeaveController controller) {
    try {
      Program program = store.loadProgram(programId, type);
      ProgramController programController = createController(program, controller);
      return programController == null ? null : new SimpleRuntimeInfo(programController, type, programId);
    } catch (Exception e) {
      return null;
    }
  }

  private ProgramController createController(Program program, WeaveController controller) {
    AbstractWeaveProgramController programController = null;
    String programId = program.getId().getId();

    switch (program.getType()) {
      case FLOW: {
        FlowSpecification flowSpec = program.getSpecification().getFlows().get(programId);
        DistributedFlowletInstanceUpdater instanceUpdater = new DistributedFlowletInstanceUpdater(
          program, controller, queueAdmin, getFlowletQueues(program, flowSpec)
        );
        programController = new FlowWeaveProgramController(programId, controller, instanceUpdater);
        break;
      }
      case PROCEDURE:
        programController = new ProcedureWeaveProgramController(programId, controller);
        break;
      case MAPREDUCE:
        programController = new MapReduceWeaveProgramController(programId, controller);
        break;
      case WORKFLOW:
        programController = new WorkflowWeaveProgramController(programId, controller);
        break;
    }
    return programController == null ? null : programController.startListen();
  }

  private Type getType(String typeName) {
    try {
      return Type.valueOf(typeName.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  // TODO (terence) : This method is part of the hack mentioned above. It should be removed when
  // FlowProgramRunner moved to run in AM.
  private Multimap<String, QueueName> getFlowletQueues(Program program, FlowSpecification flowSpec) {
    // Generate all queues specifications
    Id.Account accountId = Id.Account.from(program.getAccountId());
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecs
      = new SimpleQueueSpecificationGenerator(accountId).create(flowSpec);

    // For storing result from flowletId to queue.
    ImmutableSetMultimap.Builder<String, QueueName> resultBuilder = ImmutableSetMultimap.builder();

    // Loop through each flowlet
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      String flowletId = entry.getKey();
      long groupId = FlowUtils.generateConsumerGroupId(program, flowletId);
      int instances = entry.getValue().getInstances();

      // For each queue that the flowlet is a consumer, store the number of instances for this flowlet
      for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.column(flowletId).values())) {
        resultBuilder.put(flowletId, queueSpec.getQueueName());
      }
    }
    return resultBuilder.build();
  }

  /**
   * Reports resource usage of all the app masters of running weave programs.  Needs to be done here
   * until there is some sort of hook for running code in the weave app master.
   */
  private class AppMasterResourceReporter extends AbstractResourceReporter {

    public AppMasterResourceReporter(MetricsCollectionService metricsCollectionService) {
      super(metricsCollectionService);
    }

    @Override
    public void reportResources() {
      for (WeaveRunner.LiveInfo info : weaveRunner.lookupLive()) {
        String metricContext = getMetricContext(info);
        if (metricContext == null) {
          continue;
        }
        int containers = 0;
        int memory = 0;
        int vcores = 0;
        // will have multiple controllers if there are multiple runs of the same application
        for (WeaveController controller : info.getControllers()) {
          ResourceReport report = controller.getResourceReport();
          if (report == null) {
            continue;
          }
          containers++;
          memory += report.getAppMasterResources().getMemoryMB();
          vcores += report.getAppMasterResources().getVirtualCores();
        }
        sendMetrics(metricContext, containers, memory, vcores);
      }
    }

    private String getMetricContext(WeaveRunner.LiveInfo info) {
      Matcher matcher = APP_NAME_PATTERN.matcher(info.getApplicationName());
      if (!matcher.matches()) {
        return null;
      }

      Type type = getType(matcher.group(1));
      if (type == null) {
        return null;
      }
      Id.Program programId = Id.Program.from(matcher.group(2), matcher.group(3), matcher.group(4));
      return Joiner.on(".").join(programId.getApplicationId(),
                                 TypeId.getMetricContextId(type),
                                 programId.getId());
    }
  }

  @Override
  protected void startUp() throws Exception {
    resourceReporter.start();
  }

  @Override
  protected void shutDown() throws Exception {
    resourceReporter.stop();
  }
}
