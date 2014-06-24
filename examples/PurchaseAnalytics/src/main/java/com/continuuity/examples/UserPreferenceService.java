package com.continuuity.examples;

import com.continuuity.api.metrics.Metrics;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillSpecification;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CountDownLatch;


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

    private int getRandomPort() throws IOException {
      ServerSocket socket = new ServerSocket(0);
      try {
        return socket.getLocalPort();
      } finally {
        socket.close();
      }
    }

    private NettyHttpService setupUserLookupService(String host, int port) {
      List<HttpHandler> handlers = Lists.newArrayList();
      handlers.add(new UserLookupHandler());

      return NettyHttpService.builder().setHost(host)
        .setPort(port)
        .addHttpHandlers(handlers)
        .build();
    }

    @Override
    public void initialize (TwillContext context) {
      try {
        int port = getRandomPort();
        // start service
        service = setupUserLookupService(
          context.getHost().getCanonicalHostName(), port);
        service.startAndWait();
        runLatch = new CountDownLatch(1);
        context.announce("UserInterestsLookup", port);
      } catch (IOException e) {
        LOG.error("Error in initializing a random port");
        throw Throwables.propagate(e);
      }
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
      super.destroy();
    }

    @Override
    public void stop() {
      service.stopAndWait();
      runLatch.countDown();
    }
  }

  /**
   * Lookup Handler to handle users interest HTTP call.
   */
  @Path("/v1")
  public static final class UserLookupHandler extends AbstractHttpHandler {

    @Path("users/{user-id}/interest")
    @GET
    public void testGetTweet(HttpRequest request, HttpResponder responder, @PathParam("user-id") String id){
      String interest = getMockUserInterest(id);
      responder.sendString(HttpResponseStatus.OK, interest);
    }

    private String getMockUserInterest(String id){
      return "car";
    }

  }
}
