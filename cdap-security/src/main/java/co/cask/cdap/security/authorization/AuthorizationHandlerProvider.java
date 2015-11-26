/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.security.authorization;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;

/**
 * Provides an {@link AuthorizationPlugin} implementation based on cdap-site.xml.
 */
public class AuthorizationHandlerProvider implements Provider<AuthorizationPlugin> {

  private final Injector injector;
  private final Class<? extends AuthorizationPlugin> handlerClass;

  @Inject
  public AuthorizationHandlerProvider(Injector injector, CConfiguration conf) {
    this.injector = injector;
    this.handlerClass = conf.getClass(Constants.Security.Authorization.HANDLER_CLASS, null, AuthorizationPlugin.class);
  }

  @Override
  public AuthorizationPlugin get() {
    return injector.getInstance(handlerClass);
  }
}
