/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.service;

import com.google.common.reflect.TypeToken;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.TxCallable;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.spark.SparkHttpServiceHandlerSpecification;
import io.cdap.cdap.api.spark.service.SparkHttpServiceContext;
import io.cdap.cdap.api.spark.service.SparkHttpServiceHandler;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.common.lang.PropertyFieldSetter;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.internal.app.runtime.DataSetFieldSetter;
import io.cdap.cdap.internal.app.runtime.MetricsFieldSetter;
import io.cdap.cdap.internal.app.runtime.ThrowingRunnable;
import io.cdap.cdap.internal.app.runtime.service.http.AbstractDelegatorContext;
import io.cdap.cdap.internal.app.runtime.service.http.AbstractServiceHttpServer;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A guava Service for running a netty-http server for the {@link SparkHttpServiceHandler}.
 */
public class SparkHttpServiceServer extends AbstractServiceHttpServer<SparkHttpServiceHandler> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkHttpServiceServer.class);

  private final SparkRuntimeContext runtimeContext;
  private final SparkHttpServiceContext context;

  public SparkHttpServiceServer(SparkRuntimeContext runtimeContext, SparkHttpServiceContext context) {
    super(runtimeContext.getHostname(), runtimeContext.getProgram(),
          runtimeContext.getProgramOptions(), 0, runtimeContext.getServiceAnnouncer(), TransactionControl.EXPLICIT);
    this.runtimeContext = runtimeContext;
    this.context = context;
  }

  @Override
  protected String getRoutingPathName() {
    return ProgramType.SPARK.getCategoryName();
  }

  @Override
  protected LoggingContext getLoggingContext() {
    return runtimeContext.getLoggingContext();
  }

  @Override
  protected List<SparkHandlerDelegatorContext> createDelegatorContexts() throws Exception {
    List<SparkHandlerDelegatorContext> contexts = new ArrayList<>();
    InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);

    for (SparkHttpServiceHandlerSpecification spec : context.getSpecification().getHandlers()) {
      Class<?> handlerClass = getProgram().getClassLoader().loadClass(spec.getClassName());
      @SuppressWarnings("unchecked")
      TypeToken<SparkHttpServiceHandler> type = TypeToken.of((Class<SparkHttpServiceHandler>) handlerClass);

      MetricsContext handlerMetricsContext = runtimeContext.getProgramMetrics().childContext(
        Constants.Metrics.Tag.HANDLER, handlerClass.getSimpleName());

      contexts.add(new SparkHandlerDelegatorContext(type, instantiatorFactory, spec,
                                                    runtimeContext.getProgramMetrics(), handlerMetricsContext));
    }

    return contexts;
  }

  /**
   * Helper class for carrying information about each user handler instance.
   */
  private final class SparkHandlerDelegatorContext extends AbstractDelegatorContext<SparkHttpServiceHandler> {

    private final SparkHttpServiceHandlerSpecification spec;

    protected SparkHandlerDelegatorContext(TypeToken<SparkHttpServiceHandler> handlerType,
                                           InstantiatorFactory instantiatorFactory,
                                           SparkHttpServiceHandlerSpecification spec,
                                           MetricsContext programMetricsContext,
                                           MetricsContext handlerMetricsContext) {
      super(handlerType, instantiatorFactory, programMetricsContext, handlerMetricsContext);
      this.spec = spec;
    }

    @Override
    protected HandlerTaskExecutor createTaskExecutor(InstantiatorFactory instantiatorFactory) throws Exception {
      SparkHttpServiceHandler handler = instantiatorFactory.get(getHandlerType()).create();
      Reflections.visit(handler, getHandlerType().getType(),
                        new MetricsFieldSetter(context.getMetrics()),
                        new DataSetFieldSetter(runtimeContext.getDatasetCache()),
                        new PropertyFieldSetter(spec.getProperties()));

      return new HandlerTaskExecutor(handler) {
        @Override
        protected void initHandler(SparkHttpServiceHandler handler) throws Exception {
          // Spark service is always default with Explicit tx control
          TransactionControl txCtrl = Transactions.getTransactionControl(TransactionControl.EXPLICIT, Object.class,
                                                                         handler, "initialize",
                                                                         SparkHttpServiceContext.class);
          execute(() -> handler.initialize(context), txCtrl == TransactionControl.IMPLICIT);
        }

        @Override
        protected void destroyHandler(SparkHttpServiceHandler handler) {
          TransactionControl txCtrl = Transactions.getTransactionControl(TransactionControl.EXPLICIT,
                                                                         Object.class, handler, "destroy");
          try {
            execute(() -> handler.destroy(), txCtrl == TransactionControl.IMPLICIT);
          } catch (Throwable t) {
            // Don't propagate exception raised by destroy() method
            ProgramRunId programRunId = runtimeContext.getProgramRunId();
            LOG.error("Exception raised on destroy lifecycle method in class {} of the {} program of run {}",
                      getProgram().getMainClassName(), programRunId.getType().getPrettyName(), programRunId, t);
          }
        }

        @Override
        public void execute(ThrowingRunnable runnable, boolean transactional) throws Exception {
          if (transactional) {
            context.execute(datasetContext -> runnable.run());
          } else {
            runnable.run();
          }
        }

        @Override
        public <T> T execute(Callable<T> callable, boolean transactional) throws Exception {
          if (transactional) {
            return Transactionals.execute(context, (TxCallable<T>) datasetContext -> callable.call(), Exception.class);
          }
          return callable.call();
        }

        @Override
        public void releaseCallResources() {
          // no-op
        }

        @Override
        public Transactional getTransactional() {
          return context;
        }
      };
    }
  }
}
