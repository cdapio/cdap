/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.service.http.HttpServiceHandler;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.services.ServiceHttpServer;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.http.HttpHandler;
import co.cask.http.NettyHttpService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * An abstract base class for running netty-http service that host user service handler.
 *
 * @param <T> type of the user service handler
 */
public abstract class AbstractServiceHttpServer<T> extends AbstractIdleService {

  // The following system property key is for unit-test to alter behavior of the server to have faster test
  @VisibleForTesting
  public static final String HANDLER_CLEANUP_PERIOD_MILLIS = "cdap.service.http.handler.cleanup.millis";

  private static final Logger LOG = LoggerFactory.getLogger(AbstractServiceHttpServer.class);
  private static final long DEFAULT_HANDLER_CLEANUP_PERIOD_MILLIS = TimeUnit.SECONDS.toMillis(60);

  private final String host;
  private final Program program;
  private final ProgramOptions programOptions;
  private final ServiceAnnouncer serviceAnnouncer;
  private final List<AbstractDelegatorContext<T>> delegatorContexts;
  private final int instanceId;
  private final TransactionControl defaultTxControl;

  private NettyHttpService service;
  private Cancellable cancelDiscovery;
  private Timer timer;

  public AbstractServiceHttpServer(String host, Program program, ProgramOptions programOptions, int instanceId,
                                   ServiceAnnouncer serviceAnnouncer, TransactionControl defaultTxControl) {
    this.host = host;
    this.program = program;
    this.programOptions = programOptions;
    this.serviceAnnouncer = serviceAnnouncer;
    this.delegatorContexts = new ArrayList<>();
    this.instanceId = instanceId;
    this.defaultTxControl = defaultTxControl;
  }

  protected Program getProgram() {
    return program;
  }

  protected final int getInstanceId() {
    return instanceId;
  }

  protected abstract String getRoutingPathName();

  protected abstract LoggingContext getLoggingContext();

  protected abstract List<? extends AbstractDelegatorContext<T>> createDelegatorContexts() throws Exception;

  /**
   * Creates a {@link NettyHttpService} from the given host, and list of {@link AbstractDelegatorContext}s
   *
   * @param delegatorContexts the list {@link ServiceHttpServer.HandlerDelegatorContext}
   * @return a NettyHttpService which delegates to the {@link HttpServiceHandler}s to handle the HTTP requests
   */
  private NettyHttpService createNettyHttpService(Iterable<? extends AbstractDelegatorContext<T>> delegatorContexts) {
    // The service URI is always prefixed for routing purpose
    String pathPrefix = String.format("%s/namespaces/%s/apps/%s/%s/%s/methods",
                                      Constants.Gateway.API_VERSION_3,
                                      program.getNamespaceId(),
                                      program.getApplicationId(),
                                      getRoutingPathName(),
                                      program.getName());
    String versionedPathPrefix = String.format("%s/namespaces/%s/apps/%s/versions/%s/%s/%s/methods",
                                               Constants.Gateway.API_VERSION_3,
                                               program.getNamespaceId(),
                                               program.getApplicationId(),
                                               program.getId().getVersion(),
                                               getRoutingPathName(),
                                               program.getName());

    // Create HttpHandlers which delegate to the HttpServiceHandlers
    HttpHandlerFactory factory = new HttpHandlerFactory(pathPrefix, defaultTxControl);
    HttpHandlerFactory versionedFactory = new HttpHandlerFactory(versionedPathPrefix, defaultTxControl);
    List<HttpHandler> nettyHttpHandlers = Lists.newArrayList();
    // get the runtime args from the twill context
    for (AbstractDelegatorContext<T> context : delegatorContexts) {
      nettyHttpHandlers.add(factory.createHttpHandler(context.getHandlerType(), context,
                                                      context.getHandlerMetricsContext()));
      nettyHttpHandlers.add(versionedFactory.createHttpHandler(context.getHandlerType(), context,
                                                               context.getHandlerMetricsContext()));
    }

    NettyHttpService.Builder builder = NettyHttpService.builder(program.getName() + "-http")
      .setHost(host)
      .setPort(0)
      .setHttpHandlers(nettyHttpHandlers);

    return SystemArguments.configureNettyHttpService(programOptions.getUserArguments().asMap(), builder).build();
  }

  /**
   * Starts the {@link NettyHttpService} and announces this runnable as well.
   */
  @Override
  public void startUp() throws Exception {
    // All handlers of a Service run in the same Twill runnable and each Netty thread gets its own
    // instance of a handler (and handlerContext). Creating the logging context here ensures that the logs
    // during startup/shutdown and in each thread created are published.
    LoggingContextAccessor.setLoggingContext(getLoggingContext());

    delegatorContexts.addAll(createDelegatorContexts());
    service = createNettyHttpService(delegatorContexts);

    LOG.debug("Starting HTTP server for Service {}", program.getId());
    ProgramId programId = program.getId();
    service.start();

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
      service.stop();
    } finally {
      timer.cancel();

      // Go through all non-cleanup'ed handler and call destroy() upon them
      // At this point, there should be no call to any handler method, hence it's safe to call from this thread
      delegatorContexts.forEach(AbstractDelegatorContext::close);
      for (AbstractDelegatorContext<T> context : delegatorContexts) {
        context.close();
      }
    }
  }

  private TimerTask createHandlerDestroyTask() {
    return new TimerTask() {
      @Override
      public void run() {
        delegatorContexts.forEach(AbstractDelegatorContext::cleanUp);
      }
    };
  }
}
