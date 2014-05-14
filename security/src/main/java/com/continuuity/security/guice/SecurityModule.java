package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.security.auth.AccessToken;
import com.continuuity.security.auth.AccessTokenCodec;
import com.continuuity.security.auth.AccessTokenIdentifier;
import com.continuuity.security.auth.AccessTokenIdentifierCodec;
import com.continuuity.security.auth.AccessTokenTransformer;
import com.continuuity.security.auth.AccessTokenValidator;
import com.continuuity.security.auth.KeyIdentifier;
import com.continuuity.security.auth.KeyIdentifierCodec;
import com.continuuity.security.auth.TokenManager;
import com.continuuity.security.auth.TokenValidator;
import com.continuuity.security.io.Codec;
import com.continuuity.security.server.ExternalAuthenticationServer;
import com.continuuity.security.server.GrantAccessTokenHandler;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.HandlerList;

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
    handlerBinder.addBinding().toProvider(AuthenticationHandlerProvider.class);
    handlerBinder.addBinding().to(GrantAccessTokenHandler.class);
    bind(HandlerList.class).annotatedWith(Names.named("security.handlers"))
                           .toProvider(AuthenticationHandlerListProvider.class)
                           .in(Scopes.SINGLETON);
    bind(TokenValidator.class).to(AccessTokenValidator.class);
    bind(AccessTokenTransformer.class).in(Scopes.SINGLETON);
    expose(AccessTokenTransformer.class);
    expose(TokenValidator.class);
    expose(TokenManager.class);
    expose(ExternalAuthenticationServer.class);
    expose(new TypeLiteral<Codec<KeyIdentifier>>() { });
  }

  @Provides
  private Class<Handler> provideHandlerClass(CConfiguration configuration) throws ClassNotFoundException {
    return (Class<Handler>) configuration.getClass(Constants.Security.AUTH_HANDLER_CLASS, null, Handler.class);
  }

  private static final class AuthenticationHandlerProvider implements  Provider<Handler> {

    private final Injector injector;
    private final Class<Handler> handlerClass;

    @Inject
    private AuthenticationHandlerProvider(Injector injector, Class<Handler> handlerClass) {
      this.injector = injector;
      this.handlerClass = handlerClass;
    }

    @Override
    public Handler get() {
      return injector.getInstance(handlerClass);
    }
  }

  private static final class AuthenticationHandlerListProvider implements Provider<HandlerList> {
    private final HandlerList handlerList;

    @Inject
    public AuthenticationHandlerListProvider(@Named("security.handlers.set") Set<Handler> handlers) {
      handlerList = new HandlerList();
      Handler[] handlerArray = handlers.toArray(new Handler[handlers.size()]);
      ConstraintSecurityHandler securityHandler = (ConstraintSecurityHandler) handlerArray[0];
      Handler grantAccessTokenHandler = handlerArray[1];
      securityHandler.setHandler(grantAccessTokenHandler);
      handlerList.setHandlers(handlerArray);
    }

    @Override
    public HandlerList get() {
      return handlerList;
    }
  }

  protected abstract void bindKeyManager(Binder binder);
}
