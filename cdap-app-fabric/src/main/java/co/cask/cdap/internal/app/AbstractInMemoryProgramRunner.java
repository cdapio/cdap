/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.AbstractProgramController;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ProgramRunner that can be used to manage multiple in-memory instances of a Program.
 */
public abstract class AbstractInMemoryProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractInMemoryProgramRunner.class);

  private final String host;
  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  public AbstractInMemoryProgramRunner(CConfiguration cConf, ProgramRunnerFactory programRunnerFactory) {
    this.host = cConf.get(Constants.AppFabric.SERVER_ADDRESS);
    this.programRunnerFactory = programRunnerFactory;
  }

  /**
   * Starts all instances of a Program component.
   *
   * @param program The program to run
   * @param name Name of the component
   * @param instances Number of instances to start
   * @param runId The runId
   * @param options System and User runtime arguments
   * @param components A Table for storing the resulting ProgramControllers
   * @param type Type of ProgramRunnerFactory
   */
  private void startComponent(Program program, String name, int instances, RunId runId, ProgramOptions options,
                                Table<String, Integer, ProgramController> components, ProgramRunnerFactory.Type type) {
    for (int instanceId = 0; instanceId < instances; instanceId++) {
      ProgramOptions componentOptions = createComponentOptions(name, instanceId, instances, runId, options);
      ProgramController controller = programRunnerFactory.create(type).run(program, componentOptions);
      components.put(name, instanceId, controller);
    }
  }

  protected Table<String, Integer, ProgramController> startPrograms(Program program, RunId runId,
                                                                    ProgramOptions options,
                                                                    ProgramRunnerFactory.Type type, int numInstances) {
    Table<String, Integer, ProgramController> components = HashBasedTable.create();
    try {
      startComponent(program, program.getName(), numInstances, runId, options, components, type);
    } catch (Throwable t) {
      LOG.error("Failed to start all program instances", t);
      try {
        // Need to stop all started components
        Futures.successfulAsList(Iterables.transform(components.values(),
                                                     new Function<ProgramController, ListenableFuture<?>>() {
                                                       @Override
                                                       public ListenableFuture<?> apply(ProgramController controller) {
                                                         return controller.stop();
                                                       }
                                                     })).get();
        throw Throwables.propagate(t);
      } catch (Exception e) {
        LOG.error("Failed to stop all program instances upon startup failure.", e);
        throw Throwables.propagate(e);
      }
    }
    return components;
  }

  private ProgramOptions createComponentOptions(String name, int instanceId, int instances, RunId runId,
                                                           ProgramOptions options) {
    Map<String, String> systemOptions = Maps.newHashMap();
    systemOptions.putAll(options.getArguments().asMap());
    systemOptions.put(ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId));
    systemOptions.put(ProgramOptionConstants.INSTANCES, Integer.toString(instances));
    systemOptions.put(ProgramOptionConstants.RUN_ID, runId.getId());
    systemOptions.put(ProgramOptionConstants.HOST, host);
    return new SimpleProgramOptions(name, new BasicArguments(systemOptions), options.getUserArguments());
  }

  /**
   * ProgramController to manage multiple in-memory instances of a Program.
   */
  protected final class InMemoryProgramController extends AbstractProgramController {
    private final Table<String, Integer, ProgramController> components;
    private final Program program;
    private final ProgramSpecification spec;
    private final ProgramOptions options;
    private final Lock lock = new ReentrantLock();
    private final ProgramRunnerFactory.Type type;
    private final AtomicLong liveComponents;

    public InMemoryProgramController(Table<String, Integer, ProgramController> components,
                                     RunId runId, Program program, ProgramSpecification spec,
                                     ProgramOptions options, ProgramRunnerFactory.Type type) {
      super(program.getName(), runId);
      this.program = program;
      this.components = components;
      this.spec = spec;
      this.options = options;
      this.type = type;
      this.liveComponents = new AtomicLong(components.size());
      started();
      monitorComponents();
    }

    // Add listener to monitor completion/killed status of individual components, so that the program can be marked
    // as completed once all the components have completed/killed.
    private void monitorComponents() {
      for (ProgramController controller : components.values()) {
        controller.addListener(new AbstractListener() {
          @Override
          public void completed() {
            if (liveComponents.decrementAndGet()  == 0) {
              complete();
            }
          }
        }, Threads.SAME_THREAD_EXECUTOR);
      }
    }

    @Override
    protected void doSuspend() throws Exception {
      // No-op
    }

    @Override
    protected void doResume() throws Exception {
      // No-op
    }

    @Override
    protected void doStop() throws Exception {
      LOG.info("Stopping Program : " + spec.getName());
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(components.values(),
                              new Function<ProgramController, ListenableFuture<ProgramController>>() {
                                @Override
                                public ListenableFuture<ProgramController> apply(ProgramController input) {
                                  return input.stop();
                                }
                              }
          )).get();
      } finally {
        lock.unlock();
      }
      LOG.info("Program stopped: " + spec.getName());
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
        for (Map.Entry<String, String> entry : command.entrySet()) {
          changeInstances(entry.getKey(), Integer.valueOf(entry.getValue()));
        }
      } catch (Throwable t) {
        LOG.error(String.format("Fail to change instances: %s", command), t);
      } finally {
        lock.unlock();
      }
    }

    /**
     * Change the number of instances of the running runnable.
     * @param runnableName Name of the runnable
     * @param newCount New instance count
     * @throws java.util.concurrent.ExecutionException
     * @throws InterruptedException
     */
    private void changeInstances(String runnableName, final int newCount) throws Exception {
      Map<Integer, ProgramController> liveRunnables = components.row(runnableName);
      int liveCount = liveRunnables.size();
      if (liveCount == newCount) {
        return;
      }

      // stop any extra runnables
      if (liveCount > newCount) {
        List<ListenableFuture<ProgramController>> futures = Lists.newArrayListWithCapacity(liveCount - newCount);
        for (int instanceId = liveCount - 1; instanceId >= newCount; instanceId--) {
          futures.add(components.remove(runnableName, instanceId).stop());
        }
        Futures.allAsList(futures).get();
      }

      // create more runnable instances, if necessary.
      for (int instanceId = liveCount; instanceId < newCount; instanceId++) {
        ProgramOptions programOptions = createComponentOptions(runnableName, instanceId, newCount, getRunId(), options);
        ProgramController controller = programRunnerFactory.create(type).run(program, programOptions);
        components.put(runnableName, instanceId, controller);
      }

      liveRunnables = components.row(runnableName);
      // Update total instance count for all running runnables
      for (Map.Entry<Integer, ProgramController> entry : liveRunnables.entrySet()) {
        entry.getValue().command(ProgramOptionConstants.INSTANCES, newCount);
      }
    }
  }
}
