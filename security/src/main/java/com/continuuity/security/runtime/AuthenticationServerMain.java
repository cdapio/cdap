package com.continuuity.security.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.runtime.DaemonMain;
import com.continuuity.security.guice.SecurityModules;
import com.continuuity.security.server.ExternalAuthenticationServer;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.common.Services;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server for authenticating clients accessing Reactor.  When a client authenticates successfully, it is issued
 * an access token containing a verifiable representation of the client's identity.  Other Reactor services
 * (such as the router) can independently verify client identities based on the token contents.
 */
public class AuthenticationServerMain extends DaemonMain {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationServerMain.class);
  private ZKClientService zkClientService;
  private ExternalAuthenticationServer authServer;

  @Override
  public void init(String[] args) {
    Injector injector = Guice.createInjector(new ConfigModule(),
                                             new IOModule(),
                                             new SecurityModules().getDistributedModules(),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new ZKClientModule());
    CConfiguration configuration = injector.getInstance(CConfiguration.class);

    if (configuration.getBoolean(Constants.Security.CFG_SECURITY_ENABLED)) {
      this.zkClientService = injector.getInstance(ZKClientService.class);
      this.authServer = injector.getInstance(ExternalAuthenticationServer.class);
    }
  }

  @Override
  public void start() {
    if (authServer != null) {
      LOG.info("Starting AuthenticationServer.");
      Services.chainStart(zkClientService, authServer);
    } else {
      String error = "AuthenticationServer not started since security is disabled." +
                      " To enable security, set \"security.enabled\" = \"true\" in continuuity-site.xml" +
                      " and edit the appropriate configuration.";
      LOG.error(error);
    }
  }

  @Override
  public void stop() {
    if (authServer != null) {
      LOG.info("Stopping AuthenticationServer.");
      Futures.getUnchecked(Services.chainStop(authServer, zkClientService));
    }
  }

  @Override
  public void destroy() {
  }

  public static void main(String[] args) throws Exception {
    new AuthenticationServerMain().doMain(args);
  }
}
