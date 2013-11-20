package com.continuuity.gateway.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.passport.http.client.PassportClient;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;

import javax.annotation.Nullable;

/**
 * Guice modules for gateway authentication.
 */
public class GatewayAuthModules extends AbstractModule {
  private static boolean configured = false;

  @Override
  protected void configure() {
    if (configured) {
      return;
    }

    bind(PassportClient.class).toProvider(PassportClientProvider.class);
    bind(GatewayAuthenticator.class).toProvider(AuthenticatorProvider.class);

    configured = true;
  }

  /**
   * Provides GatewayAuthenticator.
   */
  private static class AuthenticatorProvider implements Provider<GatewayAuthenticator> {
    private final CConfiguration cConf;
    private final PassportClient passportClient;

    @Inject
    private AuthenticatorProvider(CConfiguration cConf, @Nullable PassportClient passportClient) {
      this.cConf = cConf;
      this.passportClient = passportClient;
    }

    @Singleton
    @Override
    public GatewayAuthenticator get() {
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
  }

  /**
   * Provides PassportClient.
   */
  private static class PassportClientProvider implements Provider<PassportClient> {
    private final CConfiguration cConf;

    @Inject
    private PassportClientProvider(CConfiguration cConf) {
      this.cConf = cConf;
    }

    @Singleton
    @Override
    public PassportClient get() {
      if (requireAuthentication(cConf)) {
        String passportServerUri = cConf.get(Constants.Gateway.CFG_PASSPORT_SERVER_URI);
        Preconditions.checkNotNull(passportServerUri);
        return PassportClient.create(passportServerUri);
      } else {
        return null;
      }
    }
  }

  private static boolean requireAuthentication(CConfiguration cConf) {
    return cConf.getBoolean(
      Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED,
      Constants.Gateway.CONFIG_AUTHENTICATION_REQUIRED_DEFAULT
    );
  }
}
