package com.continuuity.passport.http.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.passport.Constants;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 *
 */
public class PassportServer extends AbstractIdleService {

  private final NettyHttpService httpService;
  private static final Logger LOG = LoggerFactory.getLogger(PassportServer.class);
  private final int port;

  @Inject
  public PassportServer(CConfiguration configuration, Set<HttpHandler> handlers) {
    port = configuration.getInt(Constants.CFG_SERVER_PORT,
                                Constants.DEFAULT_SERVER_PORT);

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers);
    builder.setHost("localhost"); //always bind to localhost.
    builder.setPort(port);
    httpService = builder.build();
  }

    @Override
  protected void startUp() throws Exception {
    LOG.info("Starting passport server...");
    httpService.startAndWait();
    LOG.info("Passport server started at port {}", port);
  }


  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping passport server...");
    httpService.stopAndWait();
    LOG.info("Passport server stopped.");
  }

}
