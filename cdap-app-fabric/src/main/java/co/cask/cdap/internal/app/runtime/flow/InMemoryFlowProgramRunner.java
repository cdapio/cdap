/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.AbstractInMemoryProgramRunner;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.tephra.TransactionExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public final class InMemoryFlowProgramRunner extends AbstractInMemoryProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryFlowProgramRunner.class);

  private final Provider<FlowletProgramRunner> flowletProgramRunnerProvider;
  private final StreamAdmin streamAdmin;
  private final QueueAdmin queueAdmin;
  private final TransactionExecutorFactory txExecutorFactory;

  @Inject
  public InMemoryFlowProgramRunner(Provider<FlowletProgramRunner> flowletProgramRunnerProvider, StreamAdmin streamAdmin,
                                   QueueAdmin queueAdmin, TransactionExecutorFactory txExecutorFactory,
                                   CConfiguration cConf) {
    super(cConf);
    this.flowletProgramRunnerProvider = flowletProgramRunnerProvider;
    this.streamAdmin = streamAdmin;
    this.queueAdmin = queueAdmin;
    this.txExecutorFactory = txExecutorFactory;
  }

  @Override
  public ProgramRunner createProgramRunner() {
    return flowletProgramRunnerProvider.get();
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.FLOW, "Only FLOW process type is supported.");

    FlowSpecification flowSpec = appSpec.getFlows().get(program.getName());
    Preconditions.checkNotNull(flowSpec, "Missing FlowSpecification for %s", program.getName());

    try {
      // Launch flowlet program runners
      Multimap<String, QueueName> consumerQueues = FlowUtils.configureQueue(program, flowSpec,
                                                                            streamAdmin, queueAdmin, txExecutorFactory);
      final Table<String, Integer, ProgramController> flowlets = createFlowlets(program, options, flowSpec);
      return new FlowProgramController(flowlets, program, options, flowSpec, consumerQueues);
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
  private Table<String, Integer, ProgramController> createFlowlets(Program program, ProgramOptions options,
                                                                   FlowSpecification flowSpec) {
    Table<String, Integer, ProgramController> flowlets = HashBasedTable.create();

    try {
      for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
        ProgramOptions flowletOptions = resolveFlowletOptions(options, entry.getKey());
        int instanceCount = entry.getValue().getInstances();
        for (int instanceId = 0; instanceId < instanceCount; instanceId++) {
          flowlets.put(entry.getKey(), instanceId,
                       startFlowlet(program,
                                    createFlowletOptions(instanceId, instanceCount, flowletOptions)));
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

  private ProgramOptions resolveFlowletOptions(ProgramOptions options, String flowlet) {
    Map<String, String> systemArgs = new HashMap<>(options.getArguments().asMap());
    systemArgs.put(ProgramOptionConstants.FLOWLET_NAME, flowlet);

    return new SimpleProgramOptions(options.getProgramId(),
                                    new BasicArguments(systemArgs),
                                    new BasicArguments(RuntimeArguments.extractScope(
                                      FlowUtils.FLOWLET_SCOPE, flowlet, options.getUserArguments().asMap())));
  }

  private ProgramController startFlowlet(Program program, ProgramOptions options) {
    return createProgramRunner().run(program, options);
  }

  private ProgramOptions createFlowletOptions(int instanceId, int instances,
                                              ProgramOptions options) {

    Map<String, String> systemArgs = new HashMap<>();
    systemArgs.putAll(options.getArguments().asMap());
    systemArgs.put(ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId));
    systemArgs.put(ProgramOptionConstants.INSTANCES, Integer.toString(instances));

    return new SimpleProgramOptions(options.getProgramId(), new BasicArguments(systemArgs), options.getUserArguments());
  }

  private final class FlowProgramController extends AbstractProgramController {

    private final Table<String, Integer, ProgramController> flowlets;
    private final Program program;
    private final ProgramOptions options;
    private final FlowSpecification flowSpec;
    private final Lock lock = new ReentrantLock();
    private final Multimap<String, QueueName> consumerQueues;

    FlowProgramController(Table<String, Integer, ProgramController> flowlets, Program program, ProgramOptions options,
                          FlowSpecification flowSpec, Multimap<String, QueueName> consumerQueues) {
      super(program.getId().run(ProgramRunners.getRunId(options)));
      this.flowlets = flowlets;
      this.program = program;
      this.options = options;
      this.flowSpec = flowSpec;
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
      if (!ProgramOptionConstants.INSTANCES.equals(name) || !(value instanceof Map)) {
        return;
      }
      Map<String, String> command = (Map<String, String>) value;
      lock.lock();
      try {
        changeInstances(command.get("flowlet"), Integer.valueOf(command.get("newInstances")));
      } catch (Throwable t) {
        LOG.error(String.format("Fail to change instances: %s", command), t);
        stop();
        throw t;
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
                            FlowUtils.generateConsumerGroupId(program.getId(), flowletName), newInstanceCount,
                            streamAdmin, queueAdmin, txExecutorFactory);

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

      ProgramOptions flowletOptions = resolveFlowletOptions(options, flowletName);

      // Last create more instances
      for (int instanceId = liveCount; instanceId < newInstanceCount; instanceId++) {
        flowlets.put(flowletName, instanceId,
                     startFlowlet(program,
                                  createFlowletOptions(instanceId, newInstanceCount, flowletOptions)));
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
                            FlowUtils.generateConsumerGroupId(program.getId(), flowletName), newInstanceCount,
                            streamAdmin, queueAdmin, txExecutorFactory);

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
