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

package io.cdap.cdap.app.guice;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionAuthenticator;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionDiscoveryService;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteExecutionProxySelector;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.metadata.RemotePreferencesFetcherInternal;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.Authenticator;
import java.net.ProxySelector;

/**
 * A guice module to provide discovery service bindings for remote execution runtime.
 */
public class RemoteExecutionDiscoveryModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(ProxySelector.class).to(RemoteExecutionProxySelector.class).in(Scopes.SINGLETON);
    expose(ProxySelector.class);

    bind(Authenticator.class).to(RemoteExecutionAuthenticator.class).in(Scopes.SINGLETON);
    expose(Authenticator.class);

    bind(RemoteExecutionDiscoveryService.class).in(Scopes.SINGLETON);
    bind(DiscoveryService.class).to(RemoteExecutionDiscoveryService.class);
    bind(DiscoveryServiceClient.class).to(RemoteExecutionDiscoveryService.class);
    bind(PreferencesFetcher.class).to(RemotePreferencesFetcherInternal.class);

    expose(DiscoveryService.class);
    expose(DiscoveryServiceClient.class);
  }
}
