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

import com.google.inject.Binder;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.AccessTokenValidator;
import io.cdap.cdap.security.auth.KeyIdentifier;
import io.cdap.cdap.security.auth.KeyIdentifierCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.TokenValidator;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.auth.UserIdentityCodec;

/**
 * Guice bindings for core security related functionality including token and key management. This
 * extends {@code PrivateModule} in order to limit which classes are exposed.
 */
public abstract class CoreSecurityModule extends PrivateModule {

  public boolean requiresZKClient() {
    return false;
  }

  @Override
  protected final void configure() {
    bind(new TypeLiteral<Codec<AccessToken>>() {
    }).to(AccessTokenCodec.class).in(Scopes.SINGLETON);
    bind(new TypeLiteral<Codec<UserIdentity>>() {
    }).to(UserIdentityCodec.class).in(Scopes.SINGLETON);
    bind(new TypeLiteral<Codec<KeyIdentifier>>() {
    }).to(KeyIdentifierCodec.class).in(Scopes.SINGLETON);

    bindKeyManager(binder());
    bind(TokenManager.class).in(Scopes.SINGLETON);
    bind(TokenValidator.class).to(AccessTokenValidator.class);

    expose(TokenValidator.class);
    expose(TokenManager.class);
    expose(new TypeLiteral<Codec<AccessToken>>() {
    });
    expose(new TypeLiteral<Codec<KeyIdentifier>>() {
    });
  }

  protected abstract void bindKeyManager(Binder binder);
}
