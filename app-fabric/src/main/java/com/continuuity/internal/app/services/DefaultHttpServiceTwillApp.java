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
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Twill Application which runs a Netty Http Service with the handlers passed into the constructor.
 */
public class DefaultHttpServiceTwillApp implements TwillApplication {

  private static final Logger LOG = LoggerFactory.getLogger(HttpServiceTwillRunnable.class);

  String name;
  Iterable<HttpServiceHandler> handlers;

  public DefaultHttpServiceTwillApp(String name, Iterable<HttpServiceHandler> handlers) {
    this.name = name;
    this.handlers = handlers;
  }

  @Override
  public TwillSpecification configure() {
    LOG.debug("GOT HTTP SERVICE APP CONFIGURE");
    return TwillSpecification.Builder.with()
      .setName(name)
      .withRunnable()
      .add(new HttpServiceTwillRunnable(name, handlers))
      .noLocalFiles()
      .anyOrder()
      .build();
  }

  /**
   * Created by Shu on 7/29/14.
   */
  public static final class HttpServiceTwillRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(HttpServiceTwillRunnable.class);

    private String name;
    private Iterable<HttpServiceHandler> handlers;
    private NettyHttpService service;
    private CountDownLatch runLatch;

    public HttpServiceTwillRunnable(String name, Iterable<HttpServiceHandler> handlers) {
      LOG.debug("GOT HTTP SERVICE RUNNABLE CONSTRUCTOR");
      this.name = name;
      this.handlers = handlers;
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
    public void initialize(TwillContext context) {
      LOG.debug("GOT HTTP SERVICE RUNNABLE INITIALIZE");
      service = setupNettyHttpService(context.getHost().getCanonicalHostName());
      service.startAndWait();
      // announce the twillrunnable
      int port = service.getBindAddress().getPort();
      runLatch = new CountDownLatch(1);
      context.announce(name, port);
      LOG.debug("GOT HTTP SERVICE ANNOUNCE: {}", name, port);
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
}
