package com.continuuity.gateway.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.passport.http.client.PassportClient;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import javax.annotation.Nullable;

/**
 * Guice modules for gateway authentication.
 */
public class GatewayAuthModule extends AbstractModule {
  @Override
  protected void configure() {
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
      String passportServerUri = cConf.get(Constants.Gateway.CFG_PASSPORT_SERVER_URI);
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
}
