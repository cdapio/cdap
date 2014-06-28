/**
 * Copyright 2013-2014 Continuuity, Inc.
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

package com.continuuity.examples.purchase;

import com.continuuity.api.metrics.Metrics;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.google.common.collect.Lists;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillSpecification;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * User preference service runs a HTTP service that returns user interest for a given user id.
 * The implementation of the id to user interest is mock.
 */
public class CatalogLookupService implements TwillApplication {
  @Override
  public TwillSpecification configure() {
    return TwillSpecification.Builder.with()
      .setName("CatalogLookupService")
      .withRunnable()
      .add(new CatalogService())
      .noLocalFiles()
      .anyOrder()
      .build();
  }

  /**
   * Runnable that runs HTTP service to get catalog information.
   */
  public static final class CatalogService extends AbstractTwillRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogService.class);
    private Metrics metrics;
    private NettyHttpService service;
    private CountDownLatch runLatch;


    private NettyHttpService setupCatalogLookupService(String host) {
      List<HttpHandler> handlers = Lists.newArrayList();
      handlers.add(new ProductCatalogLookup());

      return NettyHttpService.builder().setHost(host)
        .setPort(0)
        .addHttpHandlers(handlers)
        .build();
    }

    @Override
    public void initialize(TwillContext context) {
      // start service
      service = setupCatalogLookupService(context.getHost().getCanonicalHostName());
      service.startAndWait();

      int port = service.getBindAddress().getPort();
      runLatch = new CountDownLatch(1);
      context.announce("LookupByProductId", port);
    }

    @Override
    public void run() {
      try {
        runLatch.await();
      } catch (InterruptedException e) {
        LOG.error("Caught exception in await {}", e.getCause(), e);
      }
    }

    @Override
    public void destroy() {
      service.stopAndWait();
    }

    @Override
    public void stop() {
      runLatch.countDown();
    }
  }

}
