/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.internal.app.services;

import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.service.http.HttpServiceContext;
import com.continuuity.api.service.http.HttpServiceHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
import com.continuuity.internal.app.runtime.service.http.HttpHandlerFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Shu on 7/30/14.
 */
public class HttpServiceTwillRunnable extends AbstractTwillRunnable {

  private static final Gson GSON = new Gson();
  private static final Type typeToken = new TypeToken<Iterable<HttpServiceHandler>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(HttpServiceTwillRunnable.class);
  private ClassLoader programClassLoader;

  private String name;
  private List<HttpServiceHandler> handlers;
  private NettyHttpService service;
  private CountDownLatch runLatch;
  private ConcurrentHashMap<String, String> runnableArgs;

  public HttpServiceTwillRunnable(String name, Iterable<HttpServiceHandler> handlers) {
    LOG.debug("GOT HTTP SERVICE RUNNABLE CONSTRUCTOR");
    this.name = name;
    this.handlers = (List<HttpServiceHandler>) handlers;
    this.runnableArgs = new ConcurrentHashMap<String, String>();
  }

  /**
   * Utility constructor used to instantiate the service from the program classloader.
   * @param programClassLoader classloader to instantiate the service with.
   */
  public HttpServiceTwillRunnable(ClassLoader programClassLoader) {
    LOG.debug("GOT HTTP SERVICE RUNNABLE CONSTRUCTOR PGMLOADER");
    this.programClassLoader = programClassLoader;
  }

  @Override
  public void run() {
    LOG.debug("GOT HTTP SERVICE RUNNABLE RUN");
    try {
      runLatch.await();
    } catch (InterruptedException e) {
      LOG.error("Caught exception in await {}", e.getCause(), e);
      Throwables.propagate(e);
    }
  }

  @Override
  public TwillRunnableSpecification configure() {
    LOG.debug("GOT HTTP SERVICE RUNNABLE CONFIGURE");
    runnableArgs.put("service.runnable.name", name);
    List<String> handlerNames = new ArrayList<String>();
    for (HttpServiceHandler handler : handlers) {
      LOG.debug("HANDLER CONFIGURE NAME: {}", handler.getClass().getName());
      handlerNames.add(handler.getClass().getName());
    }
    runnableArgs.put("service.runnable.handlers", GSON.toJson(handlerNames));
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.copyOf(runnableArgs))
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    LOG.debug("GOT HTTP SERVICE RUNNABLE INITIALIZE");
    runnableArgs = new ConcurrentHashMap<String, String>(context.getSpecification().getConfigs());
    if (name == null) {
      name = runnableArgs.remove("service.runnable.name");
    }
    if (handlers == null) {
      handlers = new ArrayList<HttpServiceHandler>();
      List<String> handlerNames = GSON.fromJson(runnableArgs.remove("service.runnable.handlers"),
                                                new TypeToken<List<String>>() { }.getType());
      for (String handlerName : handlerNames) {
        try {
          HttpServiceHandler handler = (HttpServiceHandler) programClassLoader.loadClass(handlerName).newInstance();
          handlers.add(handler);
        } catch (Exception e) {
          LOG.error("Could not initialize Http Service");
          Throwables.propagate(e);
        }
      }
    }
    service = setupNettyHttpService(context.getHost().getCanonicalHostName());
    service.startAndWait();

    String runnableName = name;
    if (name == null) {
      runnableName = runnableArgs.remove("service.runnable.name");
    }

    // announce the twillrunnable
    int port = service.getBindAddress().getPort();
    runLatch = new CountDownLatch(1);
    context.announce(name, port);
    LOG.debug(runnableName, name, port);
  }

  @Override
  public void destroy() {
    service.stopAndWait();
  }

  @Override
  public void stop() {
    runLatch.countDown();
  }

  private NettyHttpService setupNettyHttpService(String host) {
    // Create HttpHandlers which delegate to the HttpServiceHandlers
    HttpHandlerFactory factory = new HttpHandlerFactory();
    List<HttpHandler> nettyHttpHandlers = new ArrayList<HttpHandler>();
    for (HttpServiceHandler handler : handlers) {
      // TODO: Implement correct runtime args and spec
      nettyHttpHandlers.add(factory.createHttpHandler(handler, new HttpServiceContext() {
        @Override
        public Map<String, String> getRuntimeArguments() {
          return null;
        }

        @Override
        public ServiceDiscovered discover(String applicationId, String serviceId, String serviceName) {
          return null;
        }

        @Override
        public <T extends Closeable> T getDataSet(String name) throws DataSetInstantiationException {
          return null;
        }
      }));
    }

    return NettyHttpService.builder().setHost(host)
      .setPort(0)
      .addHttpHandlers(nettyHttpHandlers)
      .build();
  }
}
