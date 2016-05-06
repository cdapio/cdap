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

  @Inject
  protected AbstractInMemoryProgramRunner(CConfiguration cConf) {
    this.host = cConf.get(Constants.AppFabric.SERVER_ADDRESS);
  }

  /**
   * Creates a {@link ProgramRunner} that start the type of program that this program runner supports.
   */
  protected abstract ProgramRunner createProgramRunner();

  /**
   * Starts all instances of a Program component.
   * @param program The program to run
   * @param options options for the program
   * @param runId The runId
   * @param numInstances number of component instances to start
   */
  protected final ProgramController startAll(Program program, ProgramOptions options, RunId runId, int numInstances) {
    Table<String, Integer, ProgramController> components = HashBasedTable.create();
    try {
      for (int instanceId = 0; instanceId < numInstances; instanceId++) {
        ProgramOptions componentOptions = createComponentOptions(program.getName(), instanceId,
                                                                 numInstances, runId, options);
        ProgramController controller = createProgramRunner().run(program, componentOptions);
        components.put(program.getName(), instanceId, controller);
      }

      return new InMemoryProgramController(components, program, options, runId);
    } catch (Throwable t) {
      LOG.error("Failed to start all program instances", t);
      try {
        // Need to stop all started components
        Futures.successfulAsList(
          Iterables.transform(components.values(), new Function<ProgramController, ListenableFuture<?>>() {
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
  private final class InMemoryProgramController extends AbstractProgramController {
    private final Table<String, Integer, ProgramController> components;
    private final Program program;
    private final ProgramOptions options;
    private final Lock lock = new ReentrantLock();
    private final AtomicLong liveComponents;
    private volatile Throwable errorCause;

    InMemoryProgramController(Table<String, Integer, ProgramController> components,
                              Program program, ProgramOptions options, RunId runId) {
      super(program.getId(), runId);
      this.program = program;
      this.components = components;
      this.options = options;
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
            if (liveComponents.decrementAndGet() == 0) {
              // If at least one of the components failed with an error, then set the state to ERROR
              if (errorCause != null) {
                setError(errorCause);
              } else {
                complete();
              }
            }
          }

          @Override
          public void error(Throwable cause) {
            // Make a note that a component failed with an error
            errorCause = cause;
            // If the live component count goes to zero, then set the state of the controller to ERROR
            if (liveComponents.decrementAndGet() == 0) {
              setError(cause);
            }
          }
        }, Threads.SAME_THREAD_EXECUTOR);
      }
    }

    private void setError(Throwable cause) {
      error(cause);
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
      LOG.info("Stopping Program: {}", program.getName());
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(components.values(), new Function<ProgramController, ListenableFuture<?>>() {
            @Override
            public ListenableFuture<?> apply(ProgramController input) {
              return input.stop();
            }
          })).get();
      } finally {
        lock.unlock();
      }
      LOG.info("Program stopped: {}", program.getName());
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
        changeInstances(command.get("runnable"),
                        Integer.valueOf(command.get("newInstances")),
                        Integer.valueOf(command.get("oldInstances")));
      } catch (Throwable t) {
        LOG.error(String.format("Fail to change instances: %s", command), t);
        throw t;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Change the number of instances of the running runnable.
     * @param runnableName Name of the runnable
     * @param newCount New instance count
     * @param oldCount Old instance count
     * @throws java.util.concurrent.ExecutionException
     * @throws InterruptedException
     */
    private void changeInstances(String runnableName, final int newCount,
                                 // unused but makes the in-memory controller expect the same command as twill
                                 @SuppressWarnings("unused") final int oldCount) throws Exception {
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
        ProgramController controller = createProgramRunner().run(program, programOptions);
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
