/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.security.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.security.auth.AccessToken;
import co.cask.cdap.security.auth.AccessTokenCodec;
import co.cask.cdap.security.auth.AccessTokenIdentifier;
import co.cask.cdap.security.auth.AccessTokenIdentifierCodec;
import co.cask.cdap.security.auth.AccessTokenTransformer;
import co.cask.cdap.security.auth.AccessTokenValidator;
import co.cask.cdap.security.auth.KeyIdentifier;
import co.cask.cdap.security.auth.KeyIdentifierCodec;
import co.cask.cdap.security.auth.TokenManager;
import co.cask.cdap.security.auth.TokenValidator;
import co.cask.cdap.security.server.AuditLogHandler;
import co.cask.cdap.security.server.ExternalAuthenticationServer;
import co.cask.cdap.security.server.GrantAccessToken;
import com.google.common.reflect.TypeToken;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
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
    bind(new TypeLiteral<Codec<AccessTokenIdentifier>>() { }).to(AccessTokenIdentifierCodec.class).in(Scopes.SINGLETON);
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
    bind(AccessTokenTransformer.class).in(Scopes.SINGLETON);
    expose(AccessTokenTransformer.class);
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

  protected abstract void bindKeyManager(Binder binder);
}
