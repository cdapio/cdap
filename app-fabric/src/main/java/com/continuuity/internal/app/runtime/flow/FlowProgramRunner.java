/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.annotation.DisableTransaction;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public final class FlowProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlowProgramRunner.class);

  private final ProgramRunnerFactory programRunnerFactory;
  private final Map<RunId, ProgramOptions> programOptions = Maps.newHashMap();
  private final StreamAdmin streamAdmin;
  private final QueueAdmin queueAdmin;

  @Inject
  public FlowProgramRunner(ProgramRunnerFactory programRunnerFactory, StreamAdmin streamAdmin, QueueAdmin queueAdmin) {
    this.programRunnerFactory = programRunnerFactory;
    this.streamAdmin = streamAdmin;
    this.queueAdmin = queueAdmin;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.FLOW, "Only FLOW process type is supported.");

    FlowSpecification flowSpec = appSpec.getFlows().get(program.getName());
    Preconditions.checkNotNull(flowSpec, "Missing FlowSpecification for %s", program.getName());

    try {
      boolean disableTransaction = program.getMainClass().isAnnotationPresent(DisableTransaction.class);

      if (disableTransaction) {
        LOG.info("Transaction is disable for flow {}.{}", program.getApplicationId(), program.getId().getId());
      }

      // Launch flowlet program runners
      RunId runId = RunIds.generate();
      programOptions.put(runId, options);
      Multimap<String, QueueName> consumerQueues = FlowUtils.configureQueue(program, flowSpec, streamAdmin, queueAdmin);
      final Table<String, Integer, ProgramController> flowlets = createFlowlets(program, runId,
                                                                                flowSpec, disableTransaction);
      return new FlowProgramController(flowlets, runId, program, flowSpec, disableTransaction, consumerQueues);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Starts all flowlets in the flow program.
   * @param program Program to run
   * @param flowSpec The {@link FlowSpecification}.
   * @return A {@link Table} with row as flowlet id, column as instance id, cell as the {@link ProgramController}
   *         for the flowlet.
   */
  private Table<String, Integer, ProgramController> createFlowlets(Program program, RunId runId,
                                                                   FlowSpecification flowSpec,
                                                                   boolean disableTransaction) {
    Table<String, Integer, ProgramController> flowlets = HashBasedTable.create();

    try {
      for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
        int instanceCount = entry.getValue().getInstances();
        for (int instanceId = 0; instanceId < instanceCount; instanceId++) {
          flowlets.put(entry.getKey(), instanceId,
                       startFlowlet(program, createFlowletOptions(entry.getKey(), instanceId,
                                                                  instanceCount, runId, disableTransaction)));
        }
      }
    } catch (Throwable t) {
      try {
        // Need to stop all started flowlets
        Futures.successfulAsList(Iterables.transform(flowlets.values(),
          new Function<ProgramController, ListenableFuture<?>>() {
            @Override
            public ListenableFuture<?> apply(ProgramController controller) {
              return controller.stop();
            }
        })).get();
      } catch (Exception e) {
        LOG.error("Fail to stop all flowlets on failure.");
      }
      throw Throwables.propagate(t);
    }
    return flowlets;
  }

  private ProgramController startFlowlet(Program program, ProgramOptions options) {
    return programRunnerFactory.create(ProgramRunnerFactory.Type.FLOWLET)
                               .run(program, options);
  }

  private ProgramOptions createFlowletOptions(String name, int instanceId, int instances,
                                              RunId runId, boolean disableTransaction) {

    // Get the right user arguments.
    Arguments userArguments = new BasicArguments();
    if (programOptions.containsKey(runId)) {
      userArguments = programOptions.get(runId).getUserArguments();
    }

    return new SimpleProgramOptions(name, new BasicArguments(
      ImmutableMap.of(
        ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId),
        ProgramOptionConstants.INSTANCES, Integer.toString(instances),
        ProgramOptionConstants.RUN_ID, runId.getId(),
        ProgramOptionConstants.DISABLE_TRANSACTION, Boolean.toString(disableTransaction))
      ), userArguments
    );
  }

  private final class FlowProgramController extends AbstractProgramController {

    private final Table<String, Integer, ProgramController> flowlets;
    private final Program program;
    private final FlowSpecification flowSpec;
    private final Lock lock = new ReentrantLock();
    private final boolean disableTransaction;
    private final Multimap<String, QueueName> consumerQueues;

    FlowProgramController(Table<String, Integer, ProgramController> flowlets, RunId runId,
                          Program program, FlowSpecification flowSpec, boolean disableTransaction,
                          Multimap<String, QueueName> consumerQueues) {
      super(program.getName(), runId);
      this.flowlets = flowlets;
      this.program = program;
      this.flowSpec = flowSpec;
      this.disableTransaction = disableTransaction;
      this.consumerQueues = consumerQueues;
      started();
    }

    @Override
    protected void doSuspend() throws Exception {
      LOG.info("Suspending flow: " + flowSpec.getName());
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(flowlets.values(),
                              new Function<ProgramController, ListenableFuture<ProgramController>>() {
                                @Override
                                public ListenableFuture<ProgramController> apply(ProgramController input) {
                                  return input.suspend();
                                }
                              })).get();
      } finally {
        lock.unlock();
      }
      LOG.info("Flow suspended: " + flowSpec.getName());
    }

    @Override
    protected void doResume() throws Exception {
      LOG.info("Resuming flow: " + flowSpec.getName());
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(flowlets.values(),
                              new Function<ProgramController, ListenableFuture<ProgramController>>() {
                                @Override
                                public ListenableFuture<ProgramController> apply(ProgramController input) {
                                  return input.resume();
                                }
                              })).get();
      } finally {
        lock.unlock();
      }
      LOG.info("Flow resumed: " + flowSpec.getName());
    }

    @Override
    protected void doStop() throws Exception {
      LOG.info("Stopping flow: " + flowSpec.getName());
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(flowlets.values(),
                              new Function<ProgramController, ListenableFuture<ProgramController>>() {
                                @Override
                                public ListenableFuture<ProgramController> apply(ProgramController input) {
                                  return input.stop();
                                }
                              })).get();
      } finally {
        lock.unlock();
      }
      LOG.info("Flow stopped: " + flowSpec.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCommand(String name, Object value) throws Exception {
      if (!ProgramOptionConstants.FLOWLET_INSTANCES.equals(name) || !(value instanceof Map)) {
        return;
      }
      Map<String, String> command = (Map<String, String>) value;
      lock.lock();
      try {
        changeInstances(command.get("flowlet"), Integer.valueOf(command.get("newInstances")));
      } catch (Throwable t) {
        LOG.error(String.format("Fail to change instances: %s", command), t);
      } finally {
        lock.unlock();
      }
    }

    /**
     * Change the number of instances of the running flowlet. Notice that this method needs to be
     * synchronized as change of instances involves multiple steps that need to be completed all at once.
     * @param flowletName Name of the flowlet
     * @param newInstanceCount New instance count
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private synchronized void changeInstances(String flowletName, final int newInstanceCount) throws Exception {
      Map<Integer, ProgramController> liveFlowlets = flowlets.row(flowletName);
      int liveCount = liveFlowlets.size();
      if (liveCount == newInstanceCount) {
        return;
      }

      if (liveCount < newInstanceCount) {
        increaseInstances(flowletName, newInstanceCount, liveFlowlets, liveCount);
        return;
      }
      decreaseInstances(flowletName, newInstanceCount, liveFlowlets, liveCount);
    }

    private synchronized void increaseInstances(String flowletName, final int newInstanceCount,
                                                Map<Integer, ProgramController> liveFlowlets,
                                                int liveCount) throws Exception {
      // First pause all flowlets
      Futures.successfulAsList(Iterables.transform(
        liveFlowlets.values(),
        new Function<ProgramController, ListenableFuture<?>>() {
          @Override
          public ListenableFuture<?> apply(ProgramController controller) {
            return controller.suspend();
          }
        })).get();

      // Then reconfigure stream/queue consumers
      FlowUtils.reconfigure(consumerQueues.get(flowletName),
                            FlowUtils.generateConsumerGroupId(program, flowletName), newInstanceCount,
                            streamAdmin, queueAdmin);

      // Then change instance count of current flowlets
      Futures.successfulAsList(Iterables.transform(
        liveFlowlets.values(),
        new Function<ProgramController, ListenableFuture<?>>() {
          @Override
          public ListenableFuture<?> apply(ProgramController controller) {
            return controller.command(ProgramOptionConstants.INSTANCES, newInstanceCount);
          }
        })).get();

      // Next resume all current flowlets
      Futures.successfulAsList(Iterables.transform(
        liveFlowlets.values(),
        new Function<ProgramController, ListenableFuture<?>>() {
          @Override
          public ListenableFuture<?> apply(ProgramController controller) {
            return controller.resume();
          }
        })).get();

      // Last create more instances
      for (int instanceId = liveCount; instanceId < newInstanceCount; instanceId++) {
        flowlets.put(flowletName, instanceId,
                     startFlowlet(program, createFlowletOptions(flowletName, instanceId,
                                                                newInstanceCount, getRunId(), disableTransaction)));
      }
    }


    private synchronized void decreaseInstances(String flowletName, final int newInstanceCount,
                                                Map<Integer, ProgramController> liveFlowlets,
                                                int liveCount) throws Exception {
      // Shrink number of flowlets
      // First stop the extra flowlets
      List<ListenableFuture<?>> futures = Lists.newArrayListWithCapacity(liveCount - newInstanceCount);
      for (int instanceId = liveCount - 1; instanceId >= newInstanceCount; instanceId--) {
        futures.add(flowlets.remove(flowletName, instanceId).stop());
      }
      Futures.successfulAsList(futures).get();

      // Then pause all flowlets
      Futures.successfulAsList(Iterables.transform(
        liveFlowlets.values(),
        new Function<ProgramController, ListenableFuture<?>>() {
          @Override
          public ListenableFuture<?> apply(ProgramController controller) {
            return controller.suspend();
          }
        })).get();

      // Then reconfigure stream/queue consumers
      FlowUtils.reconfigure(consumerQueues.get(flowletName),
                            FlowUtils.generateConsumerGroupId(program, flowletName), newInstanceCount,
                            streamAdmin, queueAdmin);

      // Next updates instance count for each flowlets
      Futures.successfulAsList(Iterables.transform(
        liveFlowlets.values(),
        new Function<ProgramController, ListenableFuture<?>>() {
          @Override
          public ListenableFuture<?> apply(ProgramController controller) {
            return controller.command(ProgramOptionConstants.INSTANCES, newInstanceCount);
          }
        })).get();

      // Last resume all remaing flowlets
      Futures.successfulAsList(Iterables.transform(
        liveFlowlets.values(),
        new Function<ProgramController, ListenableFuture<?>>() {
          @Override
          public ListenableFuture<?> apply(ProgramController controller) {
            return controller.resume();
          }
        })).get();
    }
  }
}
