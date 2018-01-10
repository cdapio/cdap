/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.TxCallable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.artifact.ArtifactManager;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.ThrowingRunnable;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.service.http.AbstractDelegatorContext;
import co.cask.cdap.internal.app.runtime.service.http.AbstractServiceHttpServer;
import co.cask.cdap.internal.app.runtime.service.http.BasicHttpServiceContext;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.http.NettyHttpService;
import com.google.common.reflect.TypeToken;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * A guava Service which runs a {@link NettyHttpService} with a list of {@link HttpServiceHandler}s.
 */
public class ServiceHttpServer extends AbstractServiceHttpServer<HttpServiceHandler> {

  private final ServiceSpecification serviceSpecification;
  private final BasicHttpServiceContext context;
  private final CConfiguration cConf;
  private final AtomicInteger instanceCount;
  private final BasicHttpServiceContextFactory contextFactory;

  public ServiceHttpServer(String host, Program program, ProgramOptions programOptions,
                           CConfiguration cConf, ServiceSpecification spec,
                           int instanceId, int instanceCount, ServiceAnnouncer serviceAnnouncer,
                           MetricsCollectionService metricsCollectionService, DatasetFramework datasetFramework,
                           TransactionSystemClient txClient, DiscoveryServiceClient discoveryServiceClient,
                           @Nullable PluginInstantiator pluginInstantiator,
                           SecureStore secureStore, SecureStoreManager secureStoreManager,
                           MessagingService messagingService,
                           ArtifactManager artifactManager) {
    super(host, program, programOptions, instanceId, serviceAnnouncer, TransactionControl.IMPLICIT);

    this.cConf = cConf;
    this.serviceSpecification = spec;
    this.instanceCount = new AtomicInteger(instanceCount);
    this.contextFactory = createContextFactory(program, programOptions, instanceId, this.instanceCount,
                                               metricsCollectionService, datasetFramework, discoveryServiceClient,
                                               txClient, pluginInstantiator, secureStore, secureStoreManager,
                                               messagingService, artifactManager);
    this.context = contextFactory.create(null);
  }

  @Override
  protected List<HandlerDelegatorContext> createDelegatorContexts() throws Exception {
    // Constructs all handler delegator. It is for bridging ServiceHttpHandler and HttpHandler (in netty-http).
    List<HandlerDelegatorContext> delegatorContexts = new ArrayList<>();
    InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);

    for (HttpServiceHandlerSpecification handlerSpec : serviceSpecification.getHandlers().values()) {
      Class<?> handlerClass = getProgram().getClassLoader().loadClass(handlerSpec.getClassName());
      @SuppressWarnings("unchecked")
      TypeToken<HttpServiceHandler> type = TypeToken.of((Class<HttpServiceHandler>) handlerClass);

      MetricsContext metrics = context.getProgramMetrics().childContext(
        BasicHttpServiceContext.createMetricsTags(handlerSpec, getInstanceId()));
      delegatorContexts.add(new HandlerDelegatorContext(type, instantiatorFactory, handlerSpec,
                                                        contextFactory, metrics));
    }
    return delegatorContexts;

  }

  @Override
  protected String getRoutingPathName() {
    return ProgramType.SERVICE.getCategoryName();
  }

  @Override
  protected LoggingContext getLoggingContext() {
    ProgramId programId = getProgram().getId();
    return new UserServiceLoggingContext(programId.getNamespace(),
                                         programId.getApplication(),
                                         programId.getProgram(),
                                         programId.getProgram(),
                                         context.getRunId().getId(),
                                         String.valueOf(getInstanceId()));
  }

  private BasicHttpServiceContextFactory createContextFactory(Program program, ProgramOptions programOptions,
                                                              int instanceId, final AtomicInteger instanceCount,
                                                              MetricsCollectionService metricsCollectionService,
                                                              DatasetFramework datasetFramework,
                                                              DiscoveryServiceClient discoveryServiceClient,
                                                              TransactionSystemClient txClient,
                                                              @Nullable PluginInstantiator pluginInstantiator,
                                                              SecureStore secureStore,
                                                              SecureStoreManager secureStoreManager,
                                                              MessagingService messagingService,
                                                              ArtifactManager artifactManager) {
    return spec -> new BasicHttpServiceContext(program, programOptions, cConf, spec, instanceId, instanceCount,
                                               metricsCollectionService, datasetFramework, discoveryServiceClient,
                                               txClient, pluginInstantiator, secureStore, secureStoreManager,
                                               messagingService, artifactManager);
  }

  /**
   * Sets the total number of instances running for this service.
   */
  public void setInstanceCount(int instanceCount) {
    this.instanceCount.set(instanceCount);
  }

  /**
   * Helper class for carrying information about each user handler instance.
   */
  private final class HandlerDelegatorContext extends AbstractDelegatorContext<HttpServiceHandler> {

    private final HttpServiceHandlerSpecification spec;
    private final BasicHttpServiceContextFactory contextFactory;

    private HandlerDelegatorContext(TypeToken<HttpServiceHandler> handlerType,
                                    InstantiatorFactory instantiatorFactory,
                                    HttpServiceHandlerSpecification spec,
                                    BasicHttpServiceContextFactory contextFactory,
                                    MetricsContext handlerMetricsContext) {
      super(handlerType, instantiatorFactory, context.getProgramMetrics(), handlerMetricsContext);
      this.spec = spec;
      this.contextFactory = contextFactory;
    }

    @Override
    protected HandlerTaskExecutor createTaskExecutor(InstantiatorFactory instantiatorFactory) throws Exception {
      BasicHttpServiceContext context = contextFactory.create(spec);

      HttpServiceHandler handler = instantiatorFactory.get(getHandlerType()).create();
      Reflections.visit(handler, getHandlerType().getType(),
                        new MetricsFieldSetter(context.getMetrics()),
                        new DataSetFieldSetter(context),
                        new PropertyFieldSetter(spec.getProperties()));

      return new HandlerTaskExecutor(handler) {
        @Override
        protected void initHandler(HttpServiceHandler handler) throws Exception {
          TransactionControl txCtrl = Transactions.getTransactionControl(TransactionControl.IMPLICIT, Object.class,
                                                                         handler, "initialize",
                                                                         HttpServiceContext.class);
          context.initializeProgram(handler, txCtrl, true);
        }

        @Override
        protected void destroyHandler(HttpServiceHandler handler) {
          TransactionControl txCtrl = Transactions.getTransactionControl(TransactionControl.IMPLICIT,
                                                                         Object.class, handler, "destroy");
          context.destroyProgram(handler, txCtrl, true);
        }

        @Override
        public void execute(ThrowingRunnable runnable, boolean transactional) throws Exception {
          if (transactional) {
            context.execute(datasetContext -> runnable.run());
          } else {
            context.execute(runnable);
          }
        }

        @Override
        public <T> T execute(Callable<T> callable, boolean transactional) throws Exception {
          if (transactional) {
            return Transactionals.execute(context, (TxCallable<T>) datasetContext -> callable.call(), Exception.class);
          }
          return context.execute(callable);
        }

        @Override
        public Transactional getTransactional() {
          return context;
        }

        @Override
        public void close() {
          super.close();
          context.close();
        }
      };
    }
  }
}
