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

import com.continuuity.api.service.http.HttpServiceContext;
import com.continuuity.api.service.http.HttpServiceHandler;
import com.continuuity.api.service.http.HttpServiceSpecification;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.NettyHttpService;
import com.continuuity.internal.app.runtime.service.http.HttpHandlerFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Services;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class HttpServiceTwillRunnable extends AbstractTwillRunnable {

  private static final Gson GSON = new Gson();
  private static final Type HANDLER_NAMES_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Logger LOG = LoggerFactory.getLogger(HttpServiceTwillRunnable.class);
  private ClassLoader programClassLoader;

  private String name;
  private List<HttpServiceHandler> handlers;
  private NettyHttpService service;

  public HttpServiceTwillRunnable(String name, Iterable<? extends HttpServiceHandler> handlers) {
    this.name = name;
    this.handlers = ImmutableList.copyOf(handlers);
  }

  /**
   * Utility constructor used to instantiate the service from the program classloader.
   * @param programClassLoader classloader to instantiate the service with.
   */
  public HttpServiceTwillRunnable(ClassLoader programClassLoader) {
    this.programClassLoader = programClassLoader;
  }

  @Override
  public void run() {
    Future<Service.State> completion = Services.getCompletionFuture(service);
    service.startAndWait();
    // announce the twill runnable
    int port = service.getBindAddress().getPort();
    getContext().announce(name, port);
    try {
      completion.get();
    } catch (InterruptedException e) {
      LOG.debug("Got Interrupted exception in Http Service run: {}", e);
    } catch (ExecutionException e) {
      LOG.debug("Got Execution exception in Http Service run: {}", e);
    }
  }

  @Override
  public TwillRunnableSpecification configure() {
    Map<String, String> runnableArgs = new HashMap<String, String>();
    runnableArgs.put("service.runnable.name", name);
    List<String> handlerNames = new ArrayList<String>();
    for (HttpServiceHandler handler : handlers) {
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
    Map<String, String> runnableArgs = new HashMap<String, String>(context.getSpecification().getConfigs());
    name = runnableArgs.get("service.runnable.name");
    handlers = new ArrayList<HttpServiceHandler>();
    List<String> handlerNames = GSON.fromJson(runnableArgs.get("service.runnable.handlers"), HANDLER_NAMES_TYPE);
    for (String handlerName : handlerNames) {
      try {
        HttpServiceHandler handler = (HttpServiceHandler) programClassLoader.loadClass(handlerName).newInstance();
        handlers.add(handler);
      } catch (Exception e) {
        LOG.error("Could not initialize Http Service");
        Throwables.propagate(e);
      }
    }
    service = createNettyHttpService(context.getHost().getCanonicalHostName());
  }

  @Override
  public void destroy() {
  }

  @Override
  public void stop() {
    service.stop();
  }

  private NettyHttpService createNettyHttpService(String host) {
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
        public HttpServiceSpecification getSpecification() {
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
