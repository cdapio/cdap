package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.Procedure;
import com.continuuity.app.program.Program;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
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
 * {@link com.continuuity.api.procedure.Procedure#destroy()} once the procedure instance is no longer needed.
 */
final class ProcedureHandlerMethodFactory extends AbstractExecutionThreadService implements HandlerMethodFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureHandlerMethodFactory.class);
  private static final int CLEANUP_SECONDS = 60;

  private final Map<WeakReference<HandlerMethod>, ProcedureEntry> procedures;
  private final ReferenceQueue<HandlerMethod> refQueue;

  private final Program program;
  private final DataFabricFacadeFactory txAgentSupplierFactory;
  private final BasicProcedureContextFactory contextFactory;

  private Thread runThread;

  ProcedureHandlerMethodFactory(Program program, DataFabricFacadeFactory txAgentSupplierFactory,
                                BasicProcedureContextFactory contextFactory) {

    Map<WeakReference<HandlerMethod>, ProcedureEntry> map = Maps.newIdentityHashMap();
    procedures = Collections.synchronizedMap(map);
    refQueue = new ReferenceQueue<HandlerMethod>();

    this.program = program;
    this.txAgentSupplierFactory = txAgentSupplierFactory;
    this.contextFactory = contextFactory;
  }

  @Override
  public HandlerMethod create() {
    try {
      ProcedureHandlerMethod handlerMethod = new ProcedureHandlerMethod(program,
                                                                        txAgentSupplierFactory, contextFactory);
      handlerMethod.init();

      procedures.put(new WeakReference<HandlerMethod>(handlerMethod, refQueue), new ProcedureEntry(handlerMethod));

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
        // It's triggered by stop
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

  private static final class ProcedureEntry {

    private final Procedure procedure;
    private final BasicProcedureContext context;

    private ProcedureEntry(ProcedureHandlerMethod method) {
      this.procedure = method.getProcedure();
      this.context = method.getContext();
    }

    private void destroy() {
      try {
        LOG.info("Destroying procedure: " + context);
        procedure.destroy();
        LOG.info("Procedure destroyed: " + context);
      } catch (Throwable t) {
        LOG.error("Procedure throws exception during destroy.", t);
      } finally {
        context.close();
      }
    }
  }
}
