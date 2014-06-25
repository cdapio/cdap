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

package com.continuuity.examples;

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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * User preference service runs a HTTP service that returns user interest for a given user id.
 * The implementation of the id to user interest is mock.
 */
public class UserPreferenceService implements TwillApplication {
  @Override
  public TwillSpecification configure() {
    return TwillSpecification.Builder.with()
      .setName("UserPreferenceService")
      .withRunnable()
      .add(new UserInterestsLookup())
      .noLocalFiles()
      .anyOrder()
      .build();
  }

  /**
   * Runnable that runs HTTP service to mock user interests.
   */
  public static final class UserInterestsLookup extends AbstractTwillRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(UserPreferenceService.class);
    private Metrics metrics;
    private NettyHttpService service;
    private CountDownLatch runLatch;


    private NettyHttpService setupUserLookupService(String host) {
      List<HttpHandler> handlers = Lists.newArrayList();
      handlers.add(new UserInterestLookupHandler());

      return NettyHttpService.builder().setHost(host)
        .setPort(0)
        .addHttpHandlers(handlers)
        .build();
    }

    @Override
    public void initialize (TwillContext context) {
      // start service
      service = setupUserLookupService(context.getHost().getCanonicalHostName());
      service.startAndWait();

      int port = service.getBindAddress().getPort();
      runLatch = new CountDownLatch(1);
      context.announce("UserInterestsLookup", port);
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

  /**
   * Lookup Handler to handle users interest HTTP call.
   */
  @Path("/v1")
  public static final class UserInterestLookupHandler extends AbstractHttpHandler {

    @Path("users/{user-id}/interest")
    @GET
    public void userHandler(HttpRequest request, HttpResponder responder, @PathParam("user-id") String id) {
      String interest = getMockUserInterest(id);
      responder.sendString(HttpResponseStatus.OK, interest);
    }

    private String getMockUserInterest(String id) {
      return "car";
    }

  }
}
