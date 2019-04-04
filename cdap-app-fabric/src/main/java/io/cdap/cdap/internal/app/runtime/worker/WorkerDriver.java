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

package io.cdap.cdap.internal.app.runtime.worker;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.worker.Worker;
import io.cdap.cdap.api.worker.WorkerContext;
import io.cdap.cdap.api.worker.WorkerSpecification;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.common.lang.PropertyFieldSetter;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.internal.app.runtime.MetricsFieldSetter;
import io.cdap.cdap.internal.lang.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * A {@link Service} for executing {@link Worker}s.
 */
public class WorkerDriver extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerDriver.class);

  private final Program program;
  private final WorkerSpecification spec;
  private final BasicWorkerContext context;

  private Worker worker;

  public WorkerDriver(Program program, WorkerSpecification spec, BasicWorkerContext context) {
    this.program = program;
    this.spec = spec;
    this.context = context;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(context.getLoggingContext());

    // Instantiate worker instance
    Class<?> workerClass = program.getClassLoader().loadClass(spec.getClassName());
    @SuppressWarnings("unchecked")
    TypeToken<Worker> workerType = (TypeToken<Worker>) TypeToken.of(workerClass);
    worker = new InstantiatorFactory(false).get(workerType).create();

    // Fields injection
    Reflections.visit(worker, workerType.getType(),
                      new MetricsFieldSetter(context.getMetrics()),
                      new PropertyFieldSetter(spec.getProperties()));

    LOG.debug("Starting Worker Program {}", program.getId());

    // Initialize worker
    // Worker is always using Explicit transaction
    TransactionControl txControl = Transactions.getTransactionControl(TransactionControl.EXPLICIT, Worker.class,
                                                                      worker, "initialize", WorkerContext.class);
    context.initializeProgram(worker, txControl, false);
  }

  @Override
  protected void run() throws Exception {
    context.execute(worker::run);
  }

  @Override
  protected void shutDown() throws Exception {
    if (worker == null) {
      return;
    }
    // Worker is always using Explicit transaction
    TransactionControl txControl = Transactions.getTransactionControl(TransactionControl.EXPLICIT,
                                                                      Worker.class, worker, "destroy");
    context.destroyProgram(worker, txControl, false);
    context.close();
  }

  @Override
  protected void triggerShutdown() {
    if (worker == null) {
      return;
    }
    LOG.debug("Stopping Worker Program {}", program.getId());
    try {
      context.execute(worker::stop);
    } catch (Exception e) {
      LOG.warn("Exception raised when stopping Worker Program {}", program.getId());
    }
  }

  @Override
  protected Executor executor() {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread t = new Thread(command, String.format("worker-%s-%d", program.getName(), context.getInstanceId()));
        t.setDaemon(true);
        t.start();
      }
    };
  }

  public void setInstanceCount(int instanceCount) {
    context.setInstanceCount(instanceCount);
  }
}
