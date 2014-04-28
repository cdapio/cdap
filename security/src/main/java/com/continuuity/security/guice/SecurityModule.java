package com.continuuity.security.guice;

import com.continuuity.security.auth.AccessToken;
import com.continuuity.security.auth.AccessTokenCodec;
import com.continuuity.security.auth.AccessTokenIdentifier;
import com.continuuity.security.auth.AccessTokenIdentifierCodec;
import com.continuuity.security.io.Codec;
import com.continuuity.security.auth.KeyIdentifier;
import com.continuuity.security.auth.KeyIdentifierCodec;
import com.continuuity.security.auth.TokenManager;
import com.continuuity.security.server.BasicAuthenticationHandler;
import com.continuuity.security.server.ExternalAuthenticationServer;
import com.continuuity.security.server.GrantAccessTokenHandler;
import com.google.inject.Binder;
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

import java.util.Set;

/**
 * Guice bindings for security related classes.  This extends {@code PrivateModule} in order to limit which classes
 * are exposed.
 */
public abstract class SecurityModule extends PrivateModule {

  @Override
  protected final void configure() {
    bind(new TypeLiteral<Codec<AccessToken>>() { }).to(AccessTokenCodec.class).in(Scopes.SINGLETON);
    bind(new TypeLiteral<Codec<AccessTokenIdentifier>>() { }).to(AccessTokenIdentifierCodec.class).in(Scopes.SINGLETON);
    bind(new TypeLiteral<Codec<KeyIdentifier>>() { }).to(KeyIdentifierCodec.class).in(Scopes.SINGLETON);

    bindKeyManager(binder());
    bind(TokenManager.class).in(Scopes.SINGLETON);

    bind(ExternalAuthenticationServer.class).in(Scopes.SINGLETON);

    Multibinder<Handler> handlerBinder = Multibinder.newSetBinder(binder(), Handler.class,
                                                                  Names.named("security.handlers.set"));
    handlerBinder.addBinding().to(BasicAuthenticationHandler.class);
    handlerBinder.addBinding().to(GrantAccessTokenHandler.class);
    bind(HandlerList.class).annotatedWith(Names.named("security.handlers"))
                           .toProvider(HandlerListProvider.class)
                           .in(Scopes.SINGLETON);

    expose(TokenManager.class);
    expose(ExternalAuthenticationServer.class);
    expose(new TypeLiteral<Codec<KeyIdentifier>>() { });
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

  protected abstract void bindKeyManager(Binder binder);
}
