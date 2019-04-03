/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import com.google.inject.assistedinject.Assisted;

import java.util.Properties;

/**
 * Guice factory for creating {@link AuthorizationContext} instances
 */
public interface AuthorizationContextFactory {
  /**
   * Creates an {@link AuthorizationContext}.
   *
   * @param extensionProperties the properties to supply to the authorization extension
   * @return the {@link AuthorizationContext} for the extension
   */
  AuthorizationContext create(@Assisted("extension-properties") Properties extensionProperties);
}
