/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.runtime;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.apache.tephra.distributed.PooledClientProvider;
import org.apache.tephra.distributed.ThreadLocalClientProvider;
import org.apache.tephra.distributed.ThriftClientProvider;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Provides Guice binding for {@link ThriftClientProvider}.
 */
public class TransactionClientModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ThriftClientProvider.class).toProvider(ThriftClientProviderSupplier.class);
  }

  /**
   * Provides implementation of {@link ThriftClientProvider}
   * based on configuration.
   */
  @Singleton
  private static final class ThriftClientProviderSupplier implements Provider<ThriftClientProvider> {

    private final Configuration cConf;
    private DiscoveryServiceClient discoveryServiceClient;

    @Inject
    ThriftClientProviderSupplier(Configuration cConf) {
      this.cConf = cConf;
    }

    @Inject(optional = true)
    void setDiscoveryServiceClient(DiscoveryServiceClient discoveryServiceClient) {
      this.discoveryServiceClient = discoveryServiceClient;
    }

    @Override
    public ThriftClientProvider get() {
      // configure the client provider
      String provider = cConf.get(TxConstants.Service.CFG_DATA_TX_CLIENT_PROVIDER,
                                  TxConstants.Service.DEFAULT_DATA_TX_CLIENT_PROVIDER);
      ThriftClientProvider clientProvider;
      if ("pool".equals(provider)) {
        clientProvider = new PooledClientProvider(cConf, discoveryServiceClient);
      } else if ("thread-local".equals(provider)) {
        clientProvider = new ThreadLocalClientProvider(cConf, discoveryServiceClient);
      } else {
        String message = "Unknown Transaction Service Client Provider '" + provider + "'.";
        throw new IllegalArgumentException(message);
      }
      return clientProvider;
    }
  }
}
