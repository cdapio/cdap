/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.credential.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.internal.credential.CredentialProviderExtensionLoader;
import io.cdap.cdap.internal.credential.CredentialProviderLoader;
import io.cdap.cdap.internal.credential.CredentialProviderService;
import io.cdap.cdap.internal.credential.DefaultCredentialProviderService;
import io.cdap.cdap.internal.namespace.credential.DefaultNamespaceCredentialProviderService;
import io.cdap.cdap.internal.namespace.credential.NamespaceCredentialProviderService;
import io.cdap.cdap.proto.credential.CredentialProvider;
import io.cdap.cdap.proto.credential.NamespaceCredentialProvider;

/**
 * Credential provider module for AppFabric.
 */
public class MasterCredentialProviderModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(CredentialProvider.class).to(CredentialProviderService.class).in(Scopes.SINGLETON);
    bind(CredentialProviderService.class).to(DefaultCredentialProviderService.class)
        .in(Scopes.SINGLETON);
    bind(CredentialProviderLoader.class).to(CredentialProviderExtensionLoader.class);
    bind(NamespaceCredentialProvider.class).to(NamespaceCredentialProviderService.class)
        .in(Scopes.SINGLETON);
    bind(NamespaceCredentialProviderService.class)
        .to(DefaultNamespaceCredentialProviderService.class).in(Scopes.SINGLETON);
  }
}
