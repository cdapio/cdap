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

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.procedure.Procedure;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionFailureException;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for creating {@link ProcedureHandlerMethod} and also call
 * {@link co.cask.cdap.api.procedure.Procedure#destroy()} once the procedure instance is no longer needed.
 */
final class ProcedureHandlerMethodFactory extends AbstractExecutionThreadService implements HandlerMethodFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureHandlerMethodFactory.class);
  private static final int CLEANUP_SECONDS = 60;

  private final Map<WeakReference<HandlerMethod>, ProcedureEntry> procedures;
  private final ReferenceQueue<HandlerMethod> refQueue;

  private final Program program;
  private final DataFabricFacadeFactory dataFabricFacadeFactory;
  private final BasicProcedureContextFactory contextFactory;

  private Thread runThread;

  ProcedureHandlerMethodFactory(Program program, DataFabricFacadeFactory dataFabricFacadeFactory,
                                BasicProcedureContextFactory contextFactory) {

    Map<WeakReference<HandlerMethod>, ProcedureEntry> map = Maps.newIdentityHashMap();
    procedures = Collections.synchronizedMap(map);
    refQueue = new ReferenceQueue<HandlerMethod>();

    this.program = program;
    this.dataFabricFacadeFactory = dataFabricFacadeFactory;
    this.contextFactory = contextFactory;
  }

  @Override
  public HandlerMethod create() {
    try {
      BasicProcedureContext context = contextFactory.create();
      DataFabricFacade dataFabricFacade = dataFabricFacadeFactory.create(program, context.getDatasetInstantiator());
      ProcedureHandlerMethod handlerMethod = new ProcedureHandlerMethod(program, dataFabricFacade, context);
      handlerMethod.init();

      procedures.put(new WeakReference<HandlerMethod>(handlerMethod, refQueue),
                     new ProcedureEntry(handlerMethod, dataFabricFacade));

      return handlerMethod;

    } catch (ClassNotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected void startUp() throws Exception {
    runThread = Thread.currentThread();
  }

  @Override
  protected void shutDown() throws Exception {
    // Call destroy on the rest of the map
    for (ProcedureEntry entry : procedures.values()) {
      entry.destroy();
    }
    procedures.clear();
  }

  @Override
  protected void triggerShutdown() {
    runThread.interrupt();
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      Reference<? extends HandlerMethod> ref = refQueue.poll();
      while (ref != null && isRunning()) {
        procedures.remove(ref).destroy();
        ref = refQueue.poll();
      }
      try {
        TimeUnit.SECONDS.sleep(CLEANUP_SECONDS);
      } catch (InterruptedException e) {
        // It's triggered by stop; ignore and continue
        continue;
      }
    }
  }

  @Override
  protected Executor executor() {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread t = new Thread(command, "procedure-destroy-caller");
        t.setDaemon(true);
        t.start();
      }
    };
  }

  /**
   * Class for holding information for each procedure instance for calling destroy() when instance get GC or
   * destroyed explicitly.
   */
  private static final class ProcedureEntry {

    private final DataFabricFacade dataFabricFacade;
    private final Procedure procedure;
    private final BasicProcedureContext context;

    private ProcedureEntry(ProcedureHandlerMethod method, DataFabricFacade dataFabricFacade) {
      this.procedure = method.getProcedure();
      this.context = method.getContext();
      this.dataFabricFacade = dataFabricFacade;
    }

    private void destroy() {
      try {
        dataFabricFacade.createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            LOG.info("Destroying procedure: " + context);
            procedure.destroy();
            LOG.info("Procedure destroyed: " + context);
          }
        });
      } catch (TransactionFailureException e) {
        Throwable cause = e.getCause() == null ? e : e.getCause();
        LOG.error("Procedure throws exception during destroy.", cause);
      } catch (InterruptedException e) {
        // nothing to do: shutting down
      } finally {
        context.close();
      }
    }
  }
}
