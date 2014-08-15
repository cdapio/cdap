/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.service.http.DefaultHttpServiceHandlerConfigurer;
import co.cask.cdap.internal.app.runtime.service.http.HttpHandlerFactory;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.service.http.DefaultHttpServiceContext;
import co.cask.cdap.internal.service.http.DefaultHttpServiceSpecification;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

  private Metrics metrics;
  private ClassLoader programClassLoader;
  private String name;
  private List<HttpServiceHandler> handlers;
  private NettyHttpService service;

  /**
   * Instantiates this class with a name which will be used when this service is announced
   * and a list of {@link HttpServiceHandler}s used to to handle the HTTP requests.
   *
   * @param name the name which will be used to announce the service
   * @param handlers the handlers of the HTTP requests
   */
  public HttpServiceTwillRunnable(String name, Iterable<? extends HttpServiceHandler> handlers) {
    this.name = name;
    this.handlers = ImmutableList.copyOf(handlers);
  }

  /**
   * Utility constructor used to instantiate the service from the program classloader.
   *
   * @param programClassLoader the classloader to instantiate the service with
   */
  public HttpServiceTwillRunnable(ClassLoader programClassLoader) {
    this.programClassLoader = programClassLoader;
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
    Cancellable contextCancellable = getContext().announce(name, port);
    LOG.info("Announced HTTP Service");
    try {
      completion.get();
      // once the service has been stopped, don't announe it anymore.
      contextCancellable.cancel();
    } catch (InterruptedException e) {
      LOG.error("Caught exception in HTTP Service run", e);
    } catch (ExecutionException e) {
      LOG.error("Caught exception in HTTP Service run", e);
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
    runnableArgs.put(CONF_RUNNABLE, name);
    List<String> handlerNames = Lists.newArrayList();
    List<HttpServiceSpecification> specs = Lists.newArrayList();
    for (HttpServiceHandler handler : handlers) {
      handlerNames.add(handler.getClass().getName());
      // call the configure method of the HTTP Handler
      DefaultHttpServiceHandlerConfigurer configurer = new DefaultHttpServiceHandlerConfigurer(handler);
      handler.configure(configurer);
      specs.add(configurer.createHttpServiceSpec());
    }
    runnableArgs.put(CONF_HANDLER, GSON.toJson(handlerNames));
    runnableArgs.put(CONF_SPEC, GSON.toJson(specs));
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
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
    Map<String, String> runnableArgs = Maps.newHashMap(context.getSpecification().getConfigs());
    name = runnableArgs.get(CONF_RUNNABLE);
    handlers = Lists.newArrayList();
    List<String> handlerNames = GSON.fromJson(runnableArgs.get(CONF_HANDLER), HANDLER_NAMES_TYPE);
    List<HttpServiceSpecification> specs = GSON.fromJson(runnableArgs.get(CONF_SPEC), HANDLER_SPEC_TYPE);
    // we will need the context based on the spec when we create NettyHttpService
    List<HandlerContextPair> handlerContextPairs = Lists.newArrayList();
    InstantiatorFactory factory = new InstantiatorFactory(false);
    for (int i = 0; i < handlerNames.size(); ++i) {
      try {
        TypeToken<?> type = TypeToken.of(programClassLoader.loadClass(handlerNames.get(i)));
        HttpServiceHandler handler = (HttpServiceHandler) factory.get(type).create();
        // create context with spec and runtime args
        DefaultHttpServiceContext httpServiceContext =
          new DefaultHttpServiceContext(specs.get(i), context.getApplicationArguments());
        // call handler initialize method with the spec from configure time
        handler.initialize(httpServiceContext);
        // set up metrics for HttpServiceHandlers
        Reflections.visit(handler, type, new MetricsFieldSetter(metrics));
        handlerContextPairs.add(new HandlerContextPair(handler, httpServiceContext));
        handlers.add(handler);
      } catch (Exception e) {
        LOG.error("Could not initialize HTTP Service");
        Throwables.propagate(e);
      }
    }
    service = createNettyHttpService(context.getHost().getCanonicalHostName(), handlerContextPairs);
  }

  /**
   * Called when this runnable is destroyed.
   */
  @Override
  public void destroy() {
    for (HttpServiceHandler handler : handlers) {
      handler.destroy();
    }
  }

  /**
   * Stops the {@link NettyHttpService} tied with this runnable.
   */
  @Override
  public void stop() {
    service.stop();
  }

  /**
   * Creates a {@link NettyHttpService} from the given host, and list of {@link HandlerContextPair}s
   *
   * @param host the host which the service will run on
   * @param handlerContextPairs the list of pairs of HttpServiceHandlers and HttpServiceContexts
   * @return a NettyHttpService which delegates to the {@link HttpServiceHandler}s to handle the HTTP requests
   */
  private static NettyHttpService createNettyHttpService(String host, List<HandlerContextPair> handlerContextPairs) {
    // Create HttpHandlers which delegate to the HttpServiceHandlers
    HttpHandlerFactory factory = new HttpHandlerFactory();
    List<HttpHandler> nettyHttpHandlers = Lists.newArrayList();
    // get the runtime args from the twill context
    for (HandlerContextPair pair : handlerContextPairs) {
      nettyHttpHandlers.add(factory.createHttpHandler(pair.getHandler(), pair.getContext()));
    }

    return NettyHttpService.builder().setHost(host)
      .setPort(0)
      .addHttpHandlers(nettyHttpHandlers)
      .build();
  }

  /**
   * Convenience class for storing a pair of {@link HttpServiceHandler} and {@link HttpServiceContext}
   */
  private static final class HandlerContextPair {

    private final HttpServiceHandler handler;
    private final HttpServiceContext context;

    public HttpServiceHandler getHandler() {
      return handler;
    }

    public HttpServiceContext getContext() {
      return context;
    }

    /**
     * Instantiates the class with a {@link HttpServiceHandler} and {@link HttpServiceContext} pair
     *
     * @param handler the handler
     * @param context the context
     */
    public HandlerContextPair(HttpServiceHandler handler, HttpServiceContext context) {
      this.handler = handler;
      this.context = context;
    }
  }
}
