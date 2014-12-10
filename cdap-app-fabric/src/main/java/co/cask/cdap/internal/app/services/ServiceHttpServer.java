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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.service.http.BasicHttpServiceContext;
import co.cask.cdap.internal.app.runtime.service.http.DelegatorContext;
import co.cask.cdap.internal.app.runtime.service.http.HttpHandlerFactory;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.tephra.TransactionExecutor;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * A guava Service which runs a {@link NettyHttpService} with a list of {@link HttpServiceHandler}s.
 */
public class ServiceHttpServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceHttpServer.class);
  private static final long HANDLER_CLEANUP_PERIOD_MS = TimeUnit.SECONDS.toMillis(60);

  // The following two fields are for tracking GC'ed suppliers of handler and be able to call destroy on them.
  private final Map<Reference<? extends Supplier<HandlerContextPair>>, HandlerContextPair> handlerReferences;
  private final ReferenceQueue<Supplier<HandlerContextPair>> handlerReferenceQueue;

  private final String host;
  private final Program program;
  private final ServiceSpecification spec;
  private final ServiceAnnouncer serviceAnnouncer;
  private final BasicHttpServiceContextFactory contextFactory;
  private final DataFabricFacadeFactory dataFabricFacadeFactory;

  private NettyHttpService service;
  private Cancellable cancelDiscovery;
  private Timer timer;

  public ServiceHttpServer(String host, Program program,  ServiceSpecification spec, RunId runId,
                           ServiceAnnouncer serviceAnnouncer, BasicHttpServiceContextFactory contextFactory,
                           MetricsCollectionService metricsCollectionService,
                           DataFabricFacadeFactory dataFabricFacadeFactory) {
    this.host = host;
    this.program = program;
    this.spec = spec;
    this.serviceAnnouncer = serviceAnnouncer;
    this.contextFactory = contextFactory;

    this.handlerReferences = Maps.newConcurrentMap();
    this.handlerReferenceQueue = new ReferenceQueue<Supplier<HandlerContextPair>>();
    this.dataFabricFacadeFactory = dataFabricFacadeFactory;

    constructNettyHttpService(runId, metricsCollectionService);
  }

  private void constructNettyHttpService(RunId runId, MetricsCollectionService metricsCollectionService) {
    Id.Program programId = program.getId();
    // Constructs all handler delegator. It is for bridging ServiceHttpHandler and HttpHandler (in netty-http).
    List<HandlerDelegatorContext> delegatorContexts = Lists.newArrayList();
    InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);

    for (Map.Entry<String, HttpServiceHandlerSpecification> entry : spec.getHandlers().entrySet()) {
      try {
        Class<?> handlerClass = program.getClassLoader().loadClass(entry.getValue().getClassName());
        @SuppressWarnings("unchecked")
        TypeToken<HttpServiceHandler> type = TypeToken.of((Class<HttpServiceHandler>) handlerClass);
        delegatorContexts.add(new HandlerDelegatorContext(type, instantiatorFactory, entry.getValue(), contextFactory));
      } catch (Exception e) {
        LOG.error("Could not initialize HTTP Service");
        Throwables.propagate(e);
      }
    }

    // The service URI is always prefixed for routing purpose
    String pathPrefix = String.format("%s/apps/%s/services/%s/methods",
                                      Constants.Gateway.API_VERSION_2,
                                      programId.getApplicationId(),
                                      programId.getId());

    service = createNettyHttpService(runId, host, pathPrefix, delegatorContexts, metricsCollectionService);
  }

  /**
   * Starts the {@link NettyHttpService} and announces this runnable as well.
   */
  @Override
  public void startUp() {
    // All handlers of a Service run in the same Twill runnable and each Netty thread gets its own
    // instance of a handler (and handlerContext). Creating the logging context here ensures that the logs
    // during startup/shutdown and in each thread created are published.
    LoggingContextAccessor.setLoggingContext(new UserServiceLoggingContext(program.getAccountId(),
                                                                           program.getApplicationId(),
                                                                           program.getId().getId(),
                                                                           program.getId().getId()));
    LOG.debug("Starting HTTP server for Service {}", program.getId());
    Id.Program programId = program.getId();
    service.startAndWait();

    // announce the twill runnable
    int port = service.getBindAddress().getPort();
    cancelDiscovery = serviceAnnouncer.announce(getServiceName(programId), port);
    LOG.info("Announced HTTP Service for Service {} at {}:{}", programId, host, port);

    // Create a Timer thread to periodically collect handler that are no longer in used and call destroy on it
    timer = new Timer("http-handler-gc", true);
    timer.scheduleAtFixedRate(createHandlerDestroyTask(), HANDLER_CLEANUP_PERIOD_MS, HANDLER_CLEANUP_PERIOD_MS);
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    try {
      service.stopAndWait();
    } finally {
      timer.cancel();

      // Go through all non-cleanup'ed handler and call destroy() upon them
      // At this point, there should be no call to any handler method, hence it's safe to call from this thread
      for (HandlerContextPair handlerContextPair : handlerReferences.values()) {
        try {
          handlerContextPair.close();
        } catch (IOException e) {
          LOG.error("Exception raised when closing the HttpServiceHandler of class {} and it's context.",
                    handlerContextPair.getClass(), e);
        }
      }
    }
  }

  private String getServiceName(Id.Program programId) {
    return String.format("%s.%s.%s.%s",
                         ProgramType.SERVICE.name().toLowerCase(),
                         programId.getAccountId(), programId.getApplicationId(), programId.getId());
  }

  private TimerTask createHandlerDestroyTask() {
    return new TimerTask() {
      @Override
      public void run() {
        Reference<? extends Supplier<HandlerContextPair>> ref = handlerReferenceQueue.poll();
        while (ref != null) {
          HandlerContextPair handler = handlerReferences.remove(ref);
          if (handler != null) {
            try {
              handler.close();
            } catch (IOException e) {
              LOG.error("Exception raised when closing the HttpServiceHandler of class {} and it's context.",
                        handler.getClass(), e);
            }
          }
          ref = handlerReferenceQueue.poll();
        }
      }
    };
  }

  private void initHandler(final HttpServiceHandler handler, final BasicHttpServiceContext serviceContext) {
    ClassLoader classLoader = ClassLoaders.setContextClassLoader(handler.getClass().getClassLoader());
    DataFabricFacade dataFabricFacade = dataFabricFacadeFactory.create(program,
                                                                       serviceContext.getDatasetInstantiator());
    try {
      dataFabricFacade.createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          handler.initialize(serviceContext);
        }
      });
    } catch (Throwable t) {
      LOG.error("Exception raised in HttpServiceHandler.initialize of class {}", handler.getClass(), t);
      throw Throwables.propagate(t);
    } finally {
      ClassLoaders.setContextClassLoader(classLoader);
    }
  }

  private void destroyHandler(final HttpServiceHandler handler, final BasicHttpServiceContext serviceContext) {
    ClassLoader classLoader = ClassLoaders.setContextClassLoader(handler.getClass().getClassLoader());
    DataFabricFacade dataFabricFacade = dataFabricFacadeFactory.create(program,
                                                                       serviceContext.getDatasetInstantiator());
    try {
      dataFabricFacade.createTransactionExecutor().execute(new TransactionExecutor.Subroutine() {
        @Override
        public void apply() throws Exception {
          handler.destroy();
        }
      });
    } catch (Throwable t) {
      LOG.error("Exception raised in HttpServiceHandler.destroy of class {}", handler.getClass(), t);
      // Don't propagate
    } finally {
      ClassLoaders.setContextClassLoader(classLoader);
    }
  }

  /**
   * Creates a {@link NettyHttpService} from the given host, and list of {@link HandlerDelegatorContext}s
   *
   * @param host the host which the service will run on
   * @param pathPrefix a string prepended to the paths which the handlers in handlerContextPairs will bind to
   * @param delegatorContexts the list {@link HandlerDelegatorContext}
   * @param metricsCollectionService
   * @return a NettyHttpService which delegates to the {@link HttpServiceHandler}s to handle the HTTP requests
   */
  private NettyHttpService createNettyHttpService(RunId runId, String host, String pathPrefix,
                                                  Iterable<HandlerDelegatorContext> delegatorContexts,
                                                  MetricsCollectionService metricsCollectionService) {
    // Create HttpHandlers which delegate to the HttpServiceHandlers
    HttpHandlerFactory factory = new HttpHandlerFactory(pathPrefix, runId.getId(),
                                                        metricsCollectionService, getMetricsContext());
    List<HttpHandler> nettyHttpHandlers = Lists.newArrayList();
    // get the runtime args from the twill context
    for (HandlerDelegatorContext context : delegatorContexts) {
      nettyHttpHandlers.add(factory.createHttpHandler(context.getHandlerType(), context));
    }

    return NettyHttpService.builder().setHost(host)
      .setPort(0)
      .addHttpHandlers(nettyHttpHandlers)
      .build();
  }

  private String getMetricsContext() {
    return String.format("%s.%s.%s.%s", program.getApplicationId(),
                                        TypeId.getMetricContextId(ProgramType.SERVICE),
                                        program.getName(), program.getName());
  }

  /**
   * Contains a reference to a handler and it's context. Upon garbage collection of these objects, a weak reference
   * to them allows destroying the handler and closing the context (thus closing the datasets used).
   */
  private final class HandlerContextPair implements Closeable {
    private final HttpServiceHandler handler;
    private final BasicHttpServiceContext context;

    private HandlerContextPair(HttpServiceHandler handler, BasicHttpServiceContext context) {
      this.handler = handler;
      this.context = context;
    }

    private BasicHttpServiceContext getContext() {
      return context;
    }

    private HttpServiceHandler getHandler() {
      return handler;
    }

    @Override
    public void close() throws IOException {
      destroyHandler(handler, context);
      context.close();
    }
  }

  /**
   * Helper class for carrying information about each user handler instance.
   */
  private final class HandlerDelegatorContext implements DelegatorContext<HttpServiceHandler> {

    private final InstantiatorFactory instantiatorFactory;
    private final ThreadLocal<Supplier<HandlerContextPair>> handlerThreadLocal;
    private final TypeToken<HttpServiceHandler> handlerType;
    private final HttpServiceHandlerSpecification spec;
    private final BasicHttpServiceContextFactory contextFactory;

    private HandlerDelegatorContext(TypeToken<HttpServiceHandler> handlerType,
                                    InstantiatorFactory instantiatorFactory,
                                    HttpServiceHandlerSpecification spec,
                                    BasicHttpServiceContextFactory contextFactory) {
      this.handlerType = handlerType;
      this.instantiatorFactory = instantiatorFactory;
      this.handlerThreadLocal = new ThreadLocal<Supplier<HandlerContextPair>>();
      this.spec = spec;
      this.contextFactory = contextFactory;
    }

    @Override
    public HttpServiceHandler getHandler() {
      return getHandlerContextPair().getHandler();
    }

    @Override
    public BasicHttpServiceContext getServiceContext() {
      return getHandlerContextPair().getContext();
    }

    /**
     * If either a {@link HttpServiceHandler} or a {@link BasicHttpServiceContext} is requested and they aren't
     * set in the ThreadLocal, then create both and set to the ThreadLocal.
     * @return the HandlerContextPair created.
     */
    private HandlerContextPair getHandlerContextPair() {
      Supplier<HandlerContextPair> supplier = handlerThreadLocal.get();
      if (supplier != null) {
        return supplier.get();
      }

      // Instantiate the user handler and injects Metrics and Dataset fields.
      HttpServiceHandler handler = instantiatorFactory.get(handlerType).create();
      BasicHttpServiceContext context = contextFactory.create(spec);
      Reflections.visit(handler, handlerType,
                        new MetricsFieldSetter(context.getMetrics()),
                        new DataSetFieldSetter(context),
                        new PropertyFieldSetter(spec.getProperties()));
      initHandler(handler, context);
      HandlerContextPair handlerContextPair = new HandlerContextPair(handler, context);
      supplier = Suppliers.ofInstance(handlerContextPair);

      // We use GC of the supplier as a signal for us to know that a thread is gone
      // The supplier is set into the thread local, which will get GC'ed when the thread is gone.
      // Since we use a weak reference key to the supplier that points to the handler
      // (in the handlerReferences map), it won't block GC of the supplier instance.
      // We can use the weak reference, which retrieved through polling the ReferenceQueue,
      // to get back the handler and call destroy() on it.
      handlerReferences.put(new WeakReference<Supplier<HandlerContextPair>>(supplier, handlerReferenceQueue),
                            handlerContextPair);
      handlerThreadLocal.set(supplier);
      return handlerContextPair;
    }

    TypeToken<HttpServiceHandler> getHandlerType() {
      return handlerType;
    }
  }
}
