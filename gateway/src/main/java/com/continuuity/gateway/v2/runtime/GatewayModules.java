package com.continuuity.gateway.v2.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.auth.PassportVPCAuthenticator;
import com.continuuity.gateway.v2.handlers.v2.AppFabricServiceHandler;
import com.continuuity.gateway.v2.handlers.v2.PingHandler;
import com.continuuity.gateway.v2.handlers.v2.ProcedureHandler;
import com.continuuity.gateway.v2.handlers.v2.WorkflowHandler;
import com.continuuity.gateway.v2.handlers.v2.dataset.ClearFabricHandler;
import com.continuuity.gateway.v2.handlers.v2.dataset.DatasetHandler;
import com.continuuity.gateway.v2.handlers.v2.dataset.TableHandler;
import com.continuuity.gateway.v2.handlers.v2.log.LogHandler;
import com.continuuity.gateway.v2.handlers.v2.stream.StreamHandler;
import com.continuuity.metrics.guice.MetricsQueryModule;
import com.continuuity.passport.PassportConstants;
import com.continuuity.passport.http.client.PassportClient;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;

import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Guice modules for Gateway.
 */
public class GatewayModules extends RuntimeModule {

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
    return new AbstractModule() {
      @Override
      protected void configure() {
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class);
        handlerBinder.addBinding().to(StreamHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(PingHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(AppFabricServiceHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(LogHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(ProcedureHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(WorkflowHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(TableHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(DatasetHandler.class).in(Scopes.SINGLETON);
        handlerBinder.addBinding().to(ClearFabricHandler.class).in(Scopes.SINGLETON);
        install(new MetricsQueryModule());
      }

      @Provides
      @Named(Constants.Gateway.ADDRESS)
      public final InetAddress providesHostname(CConfiguration cConf) {
        return Networks.resolve(cConf.get(Constants.Gateway.ADDRESS),
                                new InetSocketAddress("localhost", 0).getAddress());
      }

      @Provides
      @Singleton
      public final GatewayAuthenticator providesAuthenticator(CConfiguration cConf,
                                                              @Nullable PassportClient passportClient) {
        GatewayAuthenticator authenticator;
        if (requireAuthentication(cConf)) {
          Preconditions.checkNotNull(passportClient, "Passport client cannot be null when authentication required");
          String clusterName = cConf.get(Constants.Gateway.CLUSTER_NAME,
                                         Constants.Gateway.CLUSTER_NAME_DEFAULT);
          authenticator = new PassportVPCAuthenticator(clusterName, passportClient);
        } else {
          authenticator = new NoAuthenticator();
        }
        return authenticator;
      }

      @Provides
      @Singleton
      public PassportClient providesPassportClient(CConfiguration cConf) {
        if (requireAuthentication(cConf)) {
          String passportServerUri = cConf.get(PassportConstants.CFG_PASSPORT_SERVER_URI);
          Preconditions.checkNotNull(passportServerUri);
          return PassportClient.create(passportServerUri);
        } else {
          return null;
        }
      }

      private boolean requireAuthentication(CConfiguration cConf) {
        return cConf.getBoolean(
          Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED,
          Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED_DEFAULT
        );
      }
    };
  }
}
