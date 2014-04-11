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
import com.continuuity.security.auth.TokenValidator;
import com.continuuity.security.auth.Validator;
import com.continuuity.security.server.BasicAuthenticationHandler;
import com.continuuity.security.server.ExternalAuthenticationServer;
import com.continuuity.security.server.GrantAccessTokenHandler;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.HandlerList;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

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

    Multibinder<Handler> handlerBinder = Multibinder.newSetBinder(binder(), Handler.class,
                                                                  Names.named("security.handlers.set"));
    handlerBinder.addBinding().to(BasicAuthenticationHandler.class);
    handlerBinder.addBinding().to(GrantAccessTokenHandler.class);

    bind(HandlerList.class).annotatedWith(Names.named("security.handlers"))
                           .toProvider(HandlerListProvider.class)
                           .in(Scopes.SINGLETON);
    bindConstant().annotatedWith(Names.named("token.validity.ms"))
                  .to(cConf.getLong(Constants.Security.TOKEN_EXPIRATION, Constants.Security.DEFAULT_TOKEN_EXPIRATION));

    expose(TokenManager.class);

    //bind(TokenValidator.class).in(Scopes.SINGLETON);
    bind(Validator.class).to(TokenValidator.class);
    expose(Validator.class);

    expose(ExternalAuthenticationServer.class);
  }

  private static final class HandlerListProvider implements Provider<HandlerList> {
    private final HandlerList handlerList;

    @Inject
    public HandlerListProvider(@Named("security.handlers.set") Set<Handler> handlers) {
      handlerList = new HandlerList();
      handlerList.setHandlers(handlers.toArray(new Handler[handlers.size()]));
    }

    @Override
    public HandlerList get() {
      return handlerList;
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
