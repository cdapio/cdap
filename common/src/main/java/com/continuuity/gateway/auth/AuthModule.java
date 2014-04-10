package com.continuuity.gateway.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.passport.http.client.PassportClient;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import javax.annotation.Nullable;

/**
 * Guice modules for authentication.
 */
public class AuthModule extends AbstractModule {
  @Override
  protected void configure() {
  }

  @Provides
  @Singleton
  public final Authenticator providesAuthenticator(CConfiguration cConf,
                                                          @Nullable Provider<PassportClient> passportClientProvider) {
    Authenticator authenticator;
    if (requireAuthentication(cConf)) {
      PassportClient passportClient;
      passportClient = passportClientProvider == null ? getPassportClient(cConf) : passportClientProvider.get();
      Preconditions.checkNotNull(passportClient, "Passport client cannot be null when authentication required");

      String clusterName = cConf.get(Constants.Gateway.CLUSTER_NAME,
                                     Constants.Gateway.CLUSTER_NAME_DEFAULT);
      authenticator = new PassportVPCAuthenticator(clusterName, passportClient);
    } else {
      authenticator = new NoAuthenticator();
    }
    return authenticator;
  }

  private PassportClient getPassportClient(CConfiguration cConf) {
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
