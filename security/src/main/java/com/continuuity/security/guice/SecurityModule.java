package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.security.auth.AccessToken;
import com.continuuity.security.auth.AccessTokenCodec;
import com.continuuity.security.auth.AccessTokenIdentifier;
import com.continuuity.security.auth.AccessTokenIdentifierCodec;
import com.continuuity.security.auth.Codec;
import com.continuuity.security.auth.KeyManager;
import com.continuuity.security.auth.TokenManager;
import com.continuuity.security.server.BasicAuthenticationHandler;
import com.continuuity.security.server.ExternalAuthenticationServer;
import com.continuuity.security.server.GrantAccessTokenHandler;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.HandlerList;

import java.security.NoSuchAlgorithmException;

/**
 * Guice bindings for security related classes.  This extends {@code PrivateModule} in order to limit which classes
 * are exposed.
 */
public class SecurityModule extends PrivateModule {
  private CConfiguration cConf = CConfiguration.create();

  @Override
  protected void configure() {
    bind(new TypeLiteral<Codec<AccessToken>>() {}).to(AccessTokenCodec.class).in(Scopes.SINGLETON);
    bind(new TypeLiteral<Codec<AccessTokenIdentifier>>() {}).to(AccessTokenIdentifierCodec.class).in(Scopes.SINGLETON);
    bind(KeyManager.class).toProvider(KeyManagerProvider.class).in(Scopes.SINGLETON);
    bind(TokenManager.class).in(Scopes.SINGLETON);

    bind(ExternalAuthenticationServer.class).in(Scopes.SINGLETON);
    bind(BasicAuthenticationHandler.class).in(Scopes.SINGLETON);
    bind(GrantAccessTokenHandler.class).in(Scopes.SINGLETON);
    bind(HandlerList.class).annotatedWith(Names.named("security.handlers"))
                           .toProvider(HandlerListProvider.class)
                           .in(Scopes.SINGLETON);
    bind(Long.class).annotatedWith(Names.named("token.validity.ms"))
                    .toInstance(cConf.getLong(Constants.Security.TOKEN_EXPIRATION,
                                              Constants.Security.DEFAULT_TOKEN_EXPIRATION));

    expose(TokenManager.class);
    expose(ExternalAuthenticationServer.class);
  }

  static class HandlerListProvider implements Provider<HandlerList> {
    private BasicAuthenticationHandler authenticationHandler;
    private GrantAccessTokenHandler grantAccessTokenHandler;

    @Inject
    public HandlerListProvider(BasicAuthenticationHandler authHandler, GrantAccessTokenHandler tokenHandler) {
      this.authenticationHandler = authHandler;
      this.grantAccessTokenHandler = tokenHandler;
    }

    @Override
    public HandlerList get() {
      HandlerList handlers = new HandlerList();
      handlers.setHandlers(new Handler[] {authenticationHandler, grantAccessTokenHandler});
      return handlers;
    }
  }

  static class KeyManagerProvider implements Provider<KeyManager> {
    private CConfiguration cConf = CConfiguration.create();

    @Inject(optional = true)
    public void setCConfiguration(CConfiguration conf) {
      this.cConf = conf;
    }

    @Override
    public KeyManager get() {
      KeyManager keyManager = new KeyManager(this.cConf);
      try {
        keyManager.init();
      } catch (NoSuchAlgorithmException nsae) {
        throw Throwables.propagate(nsae);
      }
      return keyManager;
    }
  }
}
