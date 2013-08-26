package com.continuuity.gateway.v2.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.auth.PassportVPCAuthenticator;
import com.continuuity.gateway.v2.GatewayV2Constants;
import com.continuuity.gateway.v2.handlers.PingHandler;
import com.continuuity.gateway.v2.handlers.stream.StreamHandler;
import com.continuuity.passport.PassportConstants;
import com.continuuity.passport.http.client.PassportClient;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Guice modules for Gateway.
 */
public class GatewayV2Modules extends RuntimeModule {
  private final CConfiguration cConf;

  public GatewayV2Modules(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public Module getInMemoryModules() {
    return getCommonModules();
  }

  @Override
  public Module getSingleNodeModules() {
    return getCommonModules();
  }

  @Override
  public Module getDistributedModules() {
    return getCommonModules();
  }

  private Module getCommonModules() {
    final CMetrics cMetrics = new CMetrics(MetricType.System);

    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(CMetrics.class).toInstance(cMetrics);

        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
        handlerBinder.addBinding().to(StreamHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(PingHandler.class).in(Scopes.SINGLETON);

        boolean requireAuthentication = cConf.getBoolean(
          Constants.CONFIG_AUTHENTICATION_REQUIRED, Constants.CONFIG_AUTHENTICATION_REQUIRED_DEFAULT);
        GatewayAuthenticator authenticator;
        if (requireAuthentication) {
          PassportClient passportClient = PassportClient.create(
              cConf.get(PassportConstants.CFG_PASSPORT_SERVER_URI)
            );
          String clusterName = cConf.get(Constants.CONFIG_CLUSTER_NAME, Constants.CONFIG_CLUSTER_NAME_DEFAULT);
          authenticator = new PassportVPCAuthenticator(clusterName, passportClient);
        } else {
          authenticator = new NoAuthenticator();
        }
        bind(GatewayAuthenticator.class).toInstance(authenticator);
      }

      @Provides
      @Named(GatewayV2Constants.ConfigKeys.ADDRESS)
      public final InetAddress providesHostname(CConfiguration cConf) {
        return Networks.resolve(cConf.get(GatewayV2Constants.ConfigKeys.ADDRESS),
                                new InetSocketAddress("localhost", 0).getAddress());
      }
    };
  }
}
