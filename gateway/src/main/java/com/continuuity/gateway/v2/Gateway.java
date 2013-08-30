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
public class Gateway extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(Gateway.class);

  private final NettyHttpService httpService;

  @Inject
  public Gateway(CConfiguration cConf,
                 @Named(GatewayConstants.ConfigKeys.ADDRESS) InetAddress hostname,
                 @Named(GatewayConstants.GATEWAY_V2_HTTP_HANDLERS) Set<HttpHandler> handlers) {

    NettyHttpService.Builder builder = NettyHttpService.builder();
    builder.addHttpHandlers(handlers);
    builder.setHost(hostname.getCanonicalHostName());
    builder.setPort(cConf.getInt(GatewayConstants.ConfigKeys.PORT, GatewayConstants.DEFAULT_PORT));
    builder.setConnectionBacklog(cConf.getInt(GatewayConstants.ConfigKeys.BACKLOG,
                                              GatewayConstants.DEFAULT_BACKLOG));
    builder.setExecThreadPoolSize(cConf.getInt(GatewayConstants.ConfigKeys.EXEC_THREADS,
                                               GatewayConstants.DEFAULT_EXEC_THREADS));
    builder.setBossThreadPoolSize(cConf.getInt(GatewayConstants.ConfigKeys.BOSS_THREADS,
                                               GatewayConstants.DEFAULT_BOSS_THREADS));
    builder.setWorkerThreadPoolSize(cConf.getInt(GatewayConstants.ConfigKeys.WORKER_THREADS,
                                                 GatewayConstants.DEFAULT_WORKER_THREADS));

    httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Gateway...");
    httpService.startAndWait();
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
