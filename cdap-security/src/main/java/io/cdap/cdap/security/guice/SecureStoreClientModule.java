/*
 * Copyright © 2019 Cask Data, Inc.
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

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.security.store.client.RemoteSecureStore;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * The Guice module to provide binding for secure store client.
 * This module should only be used in containers in distributed mode.
 */
public class SecureStoreClientModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(RemoteSecureStore.class).in(Scopes.SINGLETON);
    bind(SecureStore.class).to(RemoteSecureStore.class);
    bind(SecureStoreManager.class).to(RemoteSecureStore.class);
  }
}
