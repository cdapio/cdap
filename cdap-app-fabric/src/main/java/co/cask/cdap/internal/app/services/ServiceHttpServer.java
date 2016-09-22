/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.DataFabricFacade;
import co.cask.cdap.internal.app.runtime.DataFabricFacadeFactory;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.app.runtime.service.http.BasicHttpServiceContext;
import co.cask.cdap.internal.app.runtime.service.http.DelegatorContext;
import co.cask.cdap.internal.app.runtime.service.http.HttpHandlerFactory;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.logging.context.UserServiceLoggingContext;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * A guava Service which runs a {@link NettyHttpService} with a list of {@link HttpServiceHandler}s.
 */
public class ServiceHttpServer extends AbstractIdleService {

  // The following three are system property keys for unit-test to alter behavior of the server to have faster test
  @VisibleForTesting
  public static final String THREAD_POOL_SIZE = "cdap.service.http.thread.pool.size";
  @VisibleForTesting
  public static final String THREAD_KEEP_ALIVE_SECONDS = "cdap.service.http.thread.keepalive.seconds";
  @VisibleForTesting
  public static final String HANDLER_CLEANUP_PERIOD_MILLIS = "cdap.service.http.handler.cleanup.millis";

  private static final Logger LOG = LoggerFactory.getLogger(ServiceHttpServer.class);
  private static final long DEFAULT_HANDLER_CLEANUP_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(60);

  private final Program program;
  private final BasicHttpServiceContext context;
  private final AtomicInteger instanceCount;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DataFabricFacadeFactory dataFabricFacadeFactory;
  private final List<HandlerDelegatorContext> handlerContexts;
  private final NettyHttpService service;

  private Cancellable cancelDiscovery;
  private Timer timer;

  public ServiceHttpServer(String host, Program program, ProgramOptions programOptions, ServiceSpecification spec,
                           int instanceId, int instanceCount, ServiceAnnouncer serviceAnnouncer,
                           MetricsCollectionService metricsCollectionService, DatasetFramework datasetFramework,
                           DataFabricFacadeFactory dataFabricFacadeFactory, TransactionSystemClient txClient,
                           DiscoveryServiceClient discoveryServiceClient,
                           @Nullable PluginInstantiator pluginInstantiator,
                           SecureStore secureStore, SecureStoreManager secureStoreManager) {
    this.program = program;
    this.instanceCount = new AtomicInteger(instanceCount);
    this.serviceAnnouncer = serviceAnnouncer;
    this.dataFabricFacadeFactory = dataFabricFacadeFactory;
    BasicHttpServiceContextFactory contextFactory = createContextFactory(program, programOptions,
                                                                         instanceId, this.instanceCount,
                                                                         metricsCollectionService,
                                                                         datasetFramework, discoveryServiceClient,
                                                                         txClient, pluginInstantiator, secureStore,
                                                                         secureStoreManager);
    this.handlerContexts = createHandlerDelegatorContexts(program, spec, contextFactory);
    this.context = contextFactory.create(null);
    this.service = createNettyHttpService(program, host, handlerContexts, context.getProgramMetrics());
  }

  private List<HandlerDelegatorContext> createHandlerDelegatorContexts(Program program, ServiceSpecification spec,
                                                                       BasicHttpServiceContextFactory contextFactory) {
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
        throw Throwables.propagate(e);
      }
    }
    return delegatorContexts;
  }

  /**
   * Creates a {@link NettyHttpService} from the given host, and list of {@link HandlerDelegatorContext}s
   *
   * @param program Program that contains the handler
   * @param host the host which the service will run on
   * @param delegatorContexts the list {@link HandlerDelegatorContext}
   * @param metricsContext a {@link MetricsContext} for metrics collection
   *
   * @return a NettyHttpService which delegates to the {@link HttpServiceHandler}s to handle the HTTP requests
   */
  private NettyHttpService createNettyHttpService(Program program, String host,
                                                  Iterable<HandlerDelegatorContext> delegatorContexts,
                                                  MetricsContext metricsContext) {
    // The service URI is always prefixed for routing purpose
    String pathPrefix = String.format("%s/namespaces/%s/apps/%s/services/%s/methods",
                                      Constants.Gateway.API_VERSION_3,
                                      program.getNamespaceId(),
                                      program.getApplicationId(),
                                      program.getName());

    String versionId = program.getId().getVersion();
    String versionedPathPrefix = String.format("%s/namespaces/%s/apps/%s/versions/%s/services/%s/methods",
                                               Constants.Gateway.API_VERSION_3,
                                               program.getNamespaceId(),
                                               program.getApplicationId(),
                                               versionId,
                                               program.getName());

    // Create HttpHandlers which delegate to the HttpServiceHandlers
    HttpHandlerFactory factory = new HttpHandlerFactory(pathPrefix, metricsContext);
    HttpHandlerFactory versionedFactory = new HttpHandlerFactory(versionedPathPrefix, metricsContext);
    List<HttpHandler> nettyHttpHandlers = Lists.newArrayList();
    // get the runtime args from the twill context
    for (HandlerDelegatorContext context : delegatorContexts) {
      nettyHttpHandlers.add(factory.createHttpHandler(context.getHandlerType(), context));
      nettyHttpHandlers.add(versionedFactory.createHttpHandler(context.getHandlerType(), context));
    }

    NettyHttpService.Builder builder = NettyHttpService.builder()
      .setHost(host)
      .setPort(0)
      .addHttpHandlers(nettyHttpHandlers);

    // These properties are for unit-test only. Currently they are not controllable by the user program
    String threadPoolSize = System.getProperty(THREAD_POOL_SIZE);
    if (threadPoolSize != null) {
      builder.setExecThreadPoolSize(Integer.parseInt(threadPoolSize));
    }
    String threadAliveSec = System.getProperty(THREAD_KEEP_ALIVE_SECONDS);
    if (threadAliveSec != null) {
      builder.setExecThreadKeepAliveSeconds(Long.parseLong(threadAliveSec));
    }

    return builder.build();
  }

  private BasicHttpServiceContextFactory createContextFactory(final Program program,
                                                              final ProgramOptions programOptions,
                                                              final int instanceId, final AtomicInteger instanceCount,
                                                              final MetricsCollectionService metricsCollectionService,
                                                              final DatasetFramework datasetFramework,
                                                              final DiscoveryServiceClient discoveryServiceClient,
                                                              final TransactionSystemClient txClient,
                                                              @Nullable final PluginInstantiator pluginInstantiator,
                                                              final SecureStore secureStore,
                                                              final SecureStoreManager secureStoreManager) {
    return new BasicHttpServiceContextFactory() {
      @Override
      public BasicHttpServiceContext create(@Nullable HttpServiceHandlerSpecification spec) {
        return new BasicHttpServiceContext(program, programOptions, spec, instanceId, instanceCount,
                                           metricsCollectionService, datasetFramework, discoveryServiceClient,
                                           txClient, pluginInstantiator, secureStore, secureStoreManager);
      }
    };
  }

  /**
   * Starts the {@link NettyHttpService} and announces this runnable as well.
   */
  @Override
  public void startUp() {
    // All handlers of a Service run in the same Twill runnable and each Netty thread gets its own
    // instance of a handler (and handlerContext). Creating the logging context here ensures that the logs
    // during startup/shutdown and in each thread created are published.
    LoggingContextAccessor.setLoggingContext(new UserServiceLoggingContext(program.getNamespaceId(),
                                                                           program.getApplicationId(),
                                                                           program.getId().getProgram(),
                                                                           program.getId().getProgram(),
                                                                           context.getRunId().getId(),
                                                                           String.valueOf(context.getInstanceId())));
    LOG.debug("Starting HTTP server for Service {}", program.getId());
    ProgramId programId = program.getId();
    service.startAndWait();

    // announce the twill runnable
    InetSocketAddress bindAddress = service.getBindAddress();
    int port = bindAddress.getPort();
    // Announce the service with its version as the payload
    cancelDiscovery = serviceAnnouncer.announce(ServiceDiscoverable.getName(programId), port,
                                                Bytes.toBytes(programId.getVersion()));
    LOG.info("Announced HTTP Service for Service {} at {}", programId, bindAddress);

    // Create a Timer thread to periodically collect handler that are no longer in used and call destroy on it
    timer = new Timer("http-handler-gc", true);

    long cleanupPeriod = DEFAULT_HANDLER_CLEANUP_PERIOD_MILLIS;
    String cleanupPeriodProperty = System.getProperty(HANDLER_CLEANUP_PERIOD_MILLIS);
    if (cleanupPeriodProperty != null) {
      cleanupPeriod = Long.parseLong(cleanupPeriodProperty);
    }
    timer.scheduleAtFixedRate(createHandlerDestroyTask(), cleanupPeriod, cleanupPeriod);
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
      for (HandlerDelegatorContext context : handlerContexts) {
        context.shutdown();
      }
    }
  }

  public void setInstanceCount(int instanceCount) {
    this.instanceCount.set(instanceCount);
  }

  private TimerTask createHandlerDestroyTask() {
    return new TimerTask() {
      @Override
      public void run() {
        for (HandlerDelegatorContext context : handlerContexts) {
          context.cleanUp();
        }
      }
    };
  }

  private void initHandler(final HttpServiceHandler handler, final BasicHttpServiceContext serviceContext) {
    ClassLoader classLoader = setContextCombinedClassLoader(handler);
    DataFabricFacade dataFabricFacade = dataFabricFacadeFactory.create(program,
                                                                       serviceContext.getDatasetCache());
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
    ClassLoader classLoader = setContextCombinedClassLoader(handler);
    DataFabricFacade dataFabricFacade = dataFabricFacadeFactory.create(program, serviceContext.getDatasetCache());
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


  private ClassLoader setContextCombinedClassLoader(HttpServiceHandler handler) {
    return ClassLoaders.setContextClassLoader(
      new CombineClassLoader(null, ImmutableList.of(handler.getClass().getClassLoader(), getClass().getClassLoader())));
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
    public void close() {
      destroyHandler(handler, context);
      context.close();
    }
  }

  /**
   * Helper class for carrying information about each user handler instance.
   */
  private final class HandlerDelegatorContext implements DelegatorContext<HttpServiceHandler> {

    private final InstantiatorFactory instantiatorFactory;
    private final TypeToken<HttpServiceHandler> handlerType;
    private final HttpServiceHandlerSpecification spec;
    private final BasicHttpServiceContextFactory contextFactory;
    private final LoadingCache<Thread, HandlerContextPair> contextPairCache;
    private final Queue<HandlerContextPair> contextPairPool;
    // This tracks the size of the concurrent queue based on the app logic for metric purpose.
    // This is used instead of queue.size() because calling ConcurrentLinkedQueue.size() is not a constant
    // time operation, but rather linear (see the javadoc).
    private final AtomicInteger contextPairPoolSize;
    private volatile boolean shutdown;

    private HandlerDelegatorContext(TypeToken<HttpServiceHandler> handlerType,
                                    InstantiatorFactory instantiatorFactory,
                                    HttpServiceHandlerSpecification spec,
                                    BasicHttpServiceContextFactory contextFactory) {
      this.handlerType = handlerType;
      this.instantiatorFactory = instantiatorFactory;
      this.spec = spec;
      this.contextFactory = contextFactory;
      this.contextPairCache = createContextPairCache();
      this.contextPairPool = new ConcurrentLinkedQueue<>();
      this.contextPairPoolSize = new AtomicInteger();
    }

    @Override
    public HttpServiceHandler getHandler() {
      return contextPairCache.getUnchecked(Thread.currentThread()).getHandler();
    }

    @Override
    public BasicHttpServiceContext getServiceContext() {
      return contextPairCache.getUnchecked(Thread.currentThread()).getContext();
    }

    @Override
    public Cancellable capture() {
      // To capture, remove the context pair from the cache.
      // The removal listener of the cache will be triggered for this thread entry with an EXPLICIT cause
      final HandlerContextPair contextPair = contextPairCache.asMap().remove(Thread.currentThread());
      if (contextPair == null) {
        // Shouldn't happen, as the context pair should of the current thread must be in the cache
        // Otherwise, it's a bug in the system.
        throw new IllegalStateException("Handler context not found for thread " + Thread.currentThread());
      }

      final AtomicBoolean cancelled = new AtomicBoolean(false);
      return new Cancellable() {
        @Override
        public void cancel() {
          if (cancelled.compareAndSet(false, true)) {
            contextPairPool.offer(contextPair);
            // offer never return false for ConcurrentLinkedQueue
            context.getProgramMetrics().gauge("context.pool.size", contextPairPoolSize.incrementAndGet());
          } else {
            // This shouldn't happen, unless there is bug in the platform.
            // Since the context capture and release is a complicated logic, it's better throwing exception
            // to guard against potential future bug.
            throw new IllegalStateException("Captured context cannot be released twice.");
          }
        }
      };
    }

    TypeToken<HttpServiceHandler> getHandlerType() {
      return handlerType;
    }

    /**
     * Performs clean up task for the context pair cache.
     */
    void cleanUp() {
      // Invalid all cached entries if the corresponding thread is no longer running
      List<Thread> invalidKeys = new ArrayList<>();
      for (Map.Entry<Thread, HandlerContextPair> entry : contextPairCache.asMap().entrySet()) {
        if (!entry.getKey().isAlive()) {
          invalidKeys.add(entry.getKey());
        }
      }
      contextPairCache.invalidateAll(invalidKeys);
      contextPairCache.cleanUp();
    }

    /**
     * Shutdown this context delegator. All cached context pair instances will be closed.
     */
    private void shutdown() {
      shutdown = true;
      contextPairCache.invalidateAll();
      contextPairCache.cleanUp();
      for (HandlerContextPair contextPair : contextPairPool) {
        contextPair.close();
      }
      contextPairPool.clear();
    }

    private LoadingCache<Thread, HandlerContextPair> createContextPairCache() {
      return CacheBuilder.newBuilder()
        .weakKeys()
        .removalListener(new RemovalListener<Thread, HandlerContextPair>() {
          @Override
          public void onRemoval(RemovalNotification<Thread, HandlerContextPair> notification) {
            Thread thread = notification.getKey();
            HandlerContextPair contextPair = notification.getValue();
            if (contextPair == null) {
              return;
            }
            // If the removal is due to eviction (expired or GC'ed) or
            // if the thread is no longer active, close the associated context.
            if (shutdown || notification.wasEvicted() || thread == null || !thread.isAlive()) {
              contextPair.close();
            }
          }
        })
        .build(new CacheLoader<Thread, HandlerContextPair>() {
          @Override
          public HandlerContextPair load(Thread key) throws Exception {
            HandlerContextPair contextPair = contextPairPool.poll();
            if (contextPair == null) {
              return createContextPair();
            }
            context.getProgramMetrics().gauge("context.pool.size", contextPairPoolSize.decrementAndGet());
            return contextPair;
          }
        });
    }

    private HandlerContextPair createContextPair() {
      // Instantiate the user handler and injects Metrics and Dataset fields.
      HttpServiceHandler handler = instantiatorFactory.get(handlerType).create();
      BasicHttpServiceContext context = contextFactory.create(spec);
      Reflections.visit(handler, handlerType.getType(),
                        new MetricsFieldSetter(context.getMetrics()),
                        new DataSetFieldSetter(context),
                        new PropertyFieldSetter(spec.getProperties()));
      initHandler(handler, context);
      return new HandlerContextPair(handler, context);
    }
  }
}
