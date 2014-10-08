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

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.service.http.BasicHttpServiceContext;
import co.cask.cdap.internal.app.runtime.service.http.DefaultHttpServiceHandlerConfigurer;
import co.cask.cdap.internal.app.runtime.service.http.DelegatorContext;
import co.cask.cdap.internal.app.runtime.service.http.HttpHandlerFactory;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.service.http.DefaultHttpServiceSpecification;
import co.cask.cdap.internal.specification.DataSetFieldExtractor;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Services;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A Twill Runnable which runs a {@link NettyHttpService} with a list of {@link HttpServiceHandler}s.
 * This is the runnable which is run in the {@link HttpServiceTwillApplication}.
 */
public class HttpServiceTwillRunnable extends AbstractTwillRunnable {

  private static final Gson GSON = new Gson();
  private static final Type HANDLER_NAMES_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Type HANDLER_SPEC_TYPE = new TypeToken<List<DefaultHttpServiceSpecification>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(HttpServiceTwillRunnable.class);
  private static final String CONF_RUNNABLE = "service.runnable.name";
  private static final String CONF_HANDLER = "service.runnable.handlers";
  private static final String CONF_SPEC = "service.runnable.handler.spec";
  private static final String CONF_APP = "app.name";
  private static final long HANDLER_CLEANUP_PERIOD_MS = TimeUnit.SECONDS.toMillis(60);

  // The metrics field is injected at runtime
  private Metrics metrics;

  private String serviceName;
  private String appName;
  private List<HttpServiceHandler> handlers;
  private NettyHttpService service;

  // The following two fields are for tracking GC'ed suppliers of handler and be able to call destroy on them.
  private Map<Reference<? extends Supplier<HandlerContextPair>>, HandlerContextPair> handlerReferences;
  private ReferenceQueue<Supplier<HandlerContextPair>> handlerReferenceQueue;

  private Program program;
  private RunId runId;
  private Set<String> datasets;
  private String baseMetricsContext;
  private MetricsCollectionService metricsCollectionService;
  private DatasetFramework datasetFramework;
  private CConfiguration cConfiguration;
  private DiscoveryServiceClient discoveryServiceClient;
  private TransactionSystemClient transactionSystemClient;

  /**
   * Instantiates this class with a name which will be used when this service is announced
   * and a list of {@link HttpServiceHandler}s used to to handle the HTTP requests.
   *
   * @param appName the name of the app which will be used to announce the service
   * @param serviceName the name of the service which will be used to announce the service
   * @param handlers the handlers of the HTTP requests
   */
  public HttpServiceTwillRunnable(String appName, String serviceName, Iterable<? extends HttpServiceHandler> handlers,
                                  Set<String> datasets) {
    this.serviceName = serviceName;
    this.handlers = ImmutableList.copyOf(handlers);
    this.appName = appName;
    Set<String> useDatasets = Sets.newHashSet(datasets);
    // Allow datasets that have only been used via the @UseDataSet annotation.
    for (HttpServiceHandler httpServiceHandler : handlers) {
      Reflections.visit(httpServiceHandler, TypeToken.of(httpServiceHandler.getClass()),
                        new DataSetFieldExtractor(useDatasets));
    }
    this.datasets = ImmutableSet.copyOf(useDatasets);
  }

  /**
   * Utility constructor used to instantiate the service from the program classloader.
   *
   */
  public HttpServiceTwillRunnable(Program program, RunId runId,
                                  CConfiguration cConfiguration, String runnableName,
                                  MetricsCollectionService metricsCollectionService,
                                  DiscoveryServiceClient discoveryServiceClient, DatasetFramework datasetFramework,
                                  TransactionSystemClient txClient) {
    this.program = program;
    this.runId = runId;
    this.cConfiguration = cConfiguration;
    this.baseMetricsContext = String.format("%s.%s.%s.%s",
                                            program.getApplicationId(), TypeId.getMetricContextId(program.getType()),
                                            program.getName(), runnableName);
    this.metricsCollectionService = metricsCollectionService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.datasetFramework = datasetFramework;
    this.transactionSystemClient = txClient;
  }

  /**
   * Starts the {@link NettyHttpService} and announces this runnable as well.
   */
  @Override
  public void run() {
    LOG.info("In run method in HTTP Service");
    Future<Service.State> completion = Services.getCompletionFuture(service);
    service.startAndWait();
    // announce the twill runnable
    int port = service.getBindAddress().getPort();
    Cancellable contextCancellable = getContext().announce(serviceName, port);
    LOG.info("Announced HTTP Service");

    // Create a Timer thread to periodically collect handler that are no longer in used and call destroy on it
    Timer timer = new Timer("http-handler-gc", true);
    timer.scheduleAtFixedRate(createHandlerDestroyTask(), HANDLER_CLEANUP_PERIOD_MS, HANDLER_CLEANUP_PERIOD_MS);

    try {
      completion.get();
    } catch (InterruptedException e) {
      LOG.error("Caught exception in HTTP Service run", e);
    } catch (ExecutionException e) {
      LOG.error("Caught exception in HTTP Service run", e);
    } finally {
      // once the service has been stopped, don't announce it anymore.
      contextCancellable.cancel();
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

  /**
   * Configures this runnable with the name and handler names as configs.
   *
   * @return the specification for this runnable
   */
  @Override
  public TwillRunnableSpecification configure() {
    LOG.info("In configure method in HTTP Service");
    Map<String, String> runnableArgs = Maps.newHashMap();
    runnableArgs.put(CONF_RUNNABLE, serviceName);
    List<String> handlerNames = Lists.newArrayList();
    List<HttpServiceSpecification> specs = Lists.newArrayList();

    // Serialize and store the datasets that have explicitly been granted access to.
    String serializedDatasets = GSON.toJson(datasets, new TypeToken<Set<String>>() { }.getType());
    runnableArgs.put("service.datasets", serializedDatasets);

    for (HttpServiceHandler handler : handlers) {
      handlerNames.add(handler.getClass().getName());
      // call the configure method of the HTTP Handler
      DefaultHttpServiceHandlerConfigurer configurer = new DefaultHttpServiceHandlerConfigurer(handler);
      handler.configure(configurer);
      specs.add(configurer.createHttpServiceSpec());
    }
    runnableArgs.put(CONF_HANDLER, GSON.toJson(handlerNames));
    runnableArgs.put(CONF_SPEC, GSON.toJson(specs));
    runnableArgs.put(CONF_APP, appName);
    return TwillRunnableSpecification.Builder.with()
      .setName(serviceName)
      .withConfigs(ImmutableMap.copyOf(runnableArgs))
      .build();
  }

  /**
   * Initializes this runnable from the given context.
   *
   * @param context the context for initialization
   */
  @Override
  public void initialize(TwillContext context) {
    LOG.info("In initialize method in HTTP Service");
    // initialize the base class so that we can use this context later
    super.initialize(context);

    handlerReferences = Maps.newConcurrentMap();
    handlerReferenceQueue = new ReferenceQueue<Supplier<HandlerContextPair>>();

    Map<String, String> runnableArgs = Maps.newHashMap(context.getSpecification().getConfigs());
    appName = runnableArgs.get(CONF_APP);
    serviceName = runnableArgs.get(CONF_RUNNABLE);
    handlers = Lists.newArrayList();
    List<String> handlerNames = GSON.fromJson(runnableArgs.get(CONF_HANDLER), HANDLER_NAMES_TYPE);
    List<HttpServiceSpecification> specs = GSON.fromJson(runnableArgs.get(CONF_SPEC), HANDLER_SPEC_TYPE);
    datasets = GSON.fromJson(runnableArgs.get("service.datasets"), new TypeToken<Set<String>>() { }.getType());
    // we will need the context based on the spec when we create NettyHttpService
    List<HandlerDelegatorContext> delegatorContexts = Lists.newArrayList();
    InstantiatorFactory instantiatorFactory = new InstantiatorFactory(false);

    for (int i = 0; i < handlerNames.size(); ++i) {
      try {
        Class<?> handlerClass = program.getClassLoader().loadClass(handlerNames.get(i));
        @SuppressWarnings("unchecked")
        TypeToken<HttpServiceHandler> type = TypeToken.of((Class<HttpServiceHandler>) handlerClass);
        delegatorContexts.add(new HandlerDelegatorContext(type, instantiatorFactory, specs.get(i), context));
      } catch (Exception e) {
        LOG.error("Could not initialize HTTP Service");
        Throwables.propagate(e);
      }
    }
    String pathPrefix = String.format("%s/apps/%s/services/%s/methods", Constants.Gateway.GATEWAY_VERSION, appName,
                                      serviceName);
    service = createNettyHttpService(context.getHost().getCanonicalHostName(), delegatorContexts, pathPrefix);
  }

  /**
   * Called when this runnable is destroyed.
   */
  @Override
  public void destroy() {
  }

  /**
   * Stops the {@link NettyHttpService} tied with this runnable.
   */
  @Override
  public void stop() {
    service.stop();
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

  private void initHandler(HttpServiceHandler handler, HttpServiceContext serviceContext) {
    try {
      handler.initialize(serviceContext);
    } catch (Throwable t) {
      LOG.error("Exception raised in HttpServiceHandler.initialize of class {}", handler.getClass(), t);
      throw Throwables.propagate(t);
    }
  }

  private void destroyHandler(HttpServiceHandler handler) {
    try {
      handler.destroy();
    } catch (Throwable t) {
      LOG.error("Exception raised in HttpServiceHandler.destroy of class {}", handler.getClass(), t);
      // Don't propagate
    }
  }

  /**
   * Creates a {@link NettyHttpService} from the given host, and list of {@link HandlerDelegatorContext}s
   *
   * @param host the host which the service will run on
   * @param delegatorContexts the list {@link HandlerDelegatorContext}
   * @param pathPrefix a string prepended to the paths which the handlers in handlerContextPairs will bind to
   * @return a NettyHttpService which delegates to the {@link HttpServiceHandler}s to handle the HTTP requests
   */
  private NettyHttpService createNettyHttpService(String host,
                                                  Iterable<HandlerDelegatorContext> delegatorContexts,
                                                  String pathPrefix) {
    // Create HttpHandlers which delegate to the HttpServiceHandlers
    HttpHandlerFactory factory = new HttpHandlerFactory(pathPrefix);
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
      destroyHandler(handler);
      context.close();
    }
  }

  private final class HandlerDelegatorContext implements DelegatorContext<HttpServiceHandler> {

    private final InstantiatorFactory instantiatorFactory;
    private final ThreadLocal<Supplier<HandlerContextPair>> handlerThreadLocal;
    private final TypeToken<HttpServiceHandler> handlerType;
    private final HttpServiceSpecification spec;
    private final TwillContext context;

    private HandlerDelegatorContext(TypeToken<HttpServiceHandler> handlerType,
                                    InstantiatorFactory instantiatorFactory, HttpServiceSpecification spec,
                                    TwillContext context) {
      this.handlerType = handlerType;
      this.instantiatorFactory = instantiatorFactory;
      this.handlerThreadLocal = new ThreadLocal<Supplier<HandlerContextPair>>();
      this.spec = spec;
      this.context = context;
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

      String metricsContext = String.format("%s.%d", baseMetricsContext, context.getInstanceId());
      BasicHttpServiceContext httpServiceContext = new BasicHttpServiceContext(spec,
                                                                               context.getApplicationArguments(),
                                                                               program, runId,
                                                                               datasets, metricsContext,
                                                                               metricsCollectionService,
                                                                               datasetFramework,
                                                                               cConfiguration,
                                                                               discoveryServiceClient,
                                                                               transactionSystemClient);

      HttpServiceHandler handler = instantiatorFactory.get(handlerType).create();
      Reflections.visit(handler, handlerType, new MetricsFieldSetter(metrics),
                        new DataSetFieldSetter(httpServiceContext));
      initHandler(handler, httpServiceContext);
      HandlerContextPair handlerContextPair = new HandlerContextPair(handler, httpServiceContext);
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
