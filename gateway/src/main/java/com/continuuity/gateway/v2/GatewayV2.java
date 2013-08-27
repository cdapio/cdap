package com.continuuity.gateway.v2;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.http.core.NettyHttpService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Gateway implemented using the common http netty framework.
 */
public class GatewayV2 extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(GatewayV2.class);

  private final NettyHttpService httpService;

  @Inject
  public GatewayV2(CConfiguration cConf,
                   @Named(GatewayV2Constants.ConfigKeys.ADDRESS) InetAddress hostname,
                   @Named(GatewayV2Constants.GATEWAY_V2_HTTP_HANDLERS) Set<HttpHandler> handlers) {

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers);
    builder.setHost(hostname.getCanonicalHostName());
    builder.setPort(cConf.getInt(GatewayV2Constants.ConfigKeys.PORT, GatewayV2Constants.DEFAULT_PORT));
    builder.setConnectionBacklog(cConf.getInt(GatewayV2Constants.ConfigKeys.BACKLOG,
                                              GatewayV2Constants.DEFAULT_BACKLOG));
    builder.setExecThreadPoolSize(cConf.getInt(GatewayV2Constants.ConfigKeys.EXEC_THREADS,
                                               GatewayV2Constants.DEFAULT_EXEC_THREADS));

    httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Gateway...");
    State state = httpService.startAndWait();
    if (state != State.RUNNING) {
      throw new IllegalStateException("Gateway could not be started properly... terminating");
    }

    LOG.info("Gateway started successfully on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Gateway...");
    httpService.stopAndWait();
  }

  public InetSocketAddress getBindAddress() {
    return httpService.getBindAddress();
  }
}
