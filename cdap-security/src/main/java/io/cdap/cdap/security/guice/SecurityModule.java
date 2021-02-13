/*
 * Copyright © 2014-2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.security.guice;

import com.google.common.reflect.TypeToken;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.AccessTokenIdentityExtractor;
import io.cdap.cdap.security.auth.AccessTokenValidator;
import io.cdap.cdap.security.auth.AuthenticationMode;
import io.cdap.cdap.security.auth.KeyIdentifier;
import io.cdap.cdap.security.auth.KeyIdentifierCodec;
import io.cdap.cdap.security.auth.ProxyUserIdentityExtractor;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.TokenValidator;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.UserIdentityCodec;
import io.cdap.cdap.security.auth.UserIdentityExtractor;
import io.cdap.cdap.security.server.AuditLogHandler;
import io.cdap.cdap.security.server.ExternalAuthenticationServer;
import io.cdap.cdap.security.server.GrantAccessToken;
import org.eclipse.jetty.server.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice bindings for security related classes. This extends {@code PrivateModule} in order to limit which classes
 * are exposed.
 */
public abstract class SecurityModule extends PrivateModule {

  private static final Logger EXTERNAL_AUTH_AUDIT_LOG = LoggerFactory.getLogger("external-auth-access");

  @Override
  protected final void configure() {
    bind(new TypeLiteral<Codec<AccessToken>>() { }).to(AccessTokenCodec.class).in(Scopes.SINGLETON);
    bind(new TypeLiteral<Codec<UserIdentity>>() { }).to(UserIdentityCodec.class).in(Scopes.SINGLETON);
    bind(new TypeLiteral<Codec<KeyIdentifier>>() { }).to(KeyIdentifierCodec.class).in(Scopes.SINGLETON);

    bindKeyManager(binder());
    bind(TokenManager.class).in(Scopes.SINGLETON);

    bind(ExternalAuthenticationServer.class).in(Scopes.SINGLETON);

    MapBinder<String, Object> handlerBinder = MapBinder.newMapBinder(binder(), String.class, Object.class,
                                                                     Names.named("security.handlers.map"));

    handlerBinder.addBinding(ExternalAuthenticationServer.HandlerType.AUTHENTICATION_HANDLER)
                 .toProvider(AuthenticationHandlerProvider.class).in(Scopes.SINGLETON);
    handlerBinder.addBinding(ExternalAuthenticationServer.HandlerType.GRANT_TOKEN_HANDLER)
                 .to(GrantAccessToken.class).in(Scopes.SINGLETON);

    bind(AuditLogHandler.class)
      .annotatedWith(Names.named(ExternalAuthenticationServer.NAMED_EXTERNAL_AUTH))
      .toInstance(new AuditLogHandler(EXTERNAL_AUTH_AUDIT_LOG));

    bind(TokenValidator.class).to(AccessTokenValidator.class);
    bind(UserIdentityExtractor.class).annotatedWith(Names.named(AccessTokenIdentityExtractor.NAME))
      .to(AccessTokenIdentityExtractor.class);
    bind(UserIdentityExtractor.class).annotatedWith(Names.named(ProxyUserIdentityExtractor.NAME))
      .to(ProxyUserIdentityExtractor.class);
    bind(UserIdentityExtractor.class).toProvider(UserIdentityExtractorProvider.class).in(Scopes.SINGLETON);
    expose(UserIdentityExtractor.class);
    expose(TokenValidator.class);
    expose(TokenManager.class);
    expose(ExternalAuthenticationServer.class);
    expose(new TypeLiteral<Codec<KeyIdentifier>>() { });
  }

  private static final class AuthenticationHandlerProvider implements Provider<Handler> {

    private final Class<? extends Handler> handlerClass;

    @Inject
    private AuthenticationHandlerProvider(CConfiguration configuration) {
      this.handlerClass = configuration.getClass(Constants.Security.AUTH_HANDLER_CLASS, null, Handler.class);
    }

    @Override
    public Handler get() {
      // we don't instantiate the handler class via injection, to avoid giving it access to objects bound in guice,
      // such as SConfiguration
      return new InstantiatorFactory(false).get(TypeToken.of(handlerClass)).create();
    }
  }

  /**
   * Provider for the {@link UserIdentityExtractor} class which supports either the {@link AccessTokenIdentityExtractor}
   * or the {@link ProxyUserIdentityExtractor}.
   */
  private static final class UserIdentityExtractorProvider implements Provider<UserIdentityExtractor> {
    private final Provider<UserIdentityExtractor> accessTokenExtractorProvider;
    private final Provider<UserIdentityExtractor> proxyExtractorProvider;
    private final AuthenticationMode mode;

    @Inject
    private UserIdentityExtractorProvider(CConfiguration configuration,
                                          @Named(AccessTokenIdentityExtractor.NAME)
                                            Provider<UserIdentityExtractor> accessTokenExtractorProvider,
                                          @Named(ProxyUserIdentityExtractor.NAME)
                                            Provider<UserIdentityExtractor> proxyExtractorProvider) {
      this.accessTokenExtractorProvider = accessTokenExtractorProvider;
      this.proxyExtractorProvider = proxyExtractorProvider;
      this.mode = configuration.getEnum(Constants.Security.Authentication.AUTHENTICATION_MODE,
                                        AuthenticationMode.MANAGED);
    }

    @Override
    public UserIdentityExtractor get() {
      if (mode.equals(AuthenticationMode.PROXY)) {
        return proxyExtractorProvider.get();
      } else {
        return accessTokenExtractorProvider.get();
      }
    }
  }

  protected abstract void bindKeyManager(Binder binder);
}
