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

package co.cask.cdap.data.runtime;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.tephra.TxConstants;
import co.cask.tephra.distributed.PooledClientProvider;
import co.cask.tephra.distributed.ThreadLocalClientProvider;
import co.cask.tephra.distributed.ThriftClientProvider;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

/**
 * Provides implementation of {@link ThriftClientProvider} based on configuration.
 */
@Singleton
public final class ThriftClientProviderSupplier implements Provider<ThriftClientProvider> {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftClientProviderSupplier.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private DiscoveryServiceClient discoveryServiceClient;

  @Inject
  ThriftClientProviderSupplier(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
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
      clientProvider = new PooledClientProvider(hConf, discoveryServiceClient);
    } else if ("thread-local".equals(provider)) {
      clientProvider = new ThreadLocalClientProvider(hConf, discoveryServiceClient);
    } else {
      String message = "Unknown Transaction Service Client Provider '" + provider + "'.";
      LOG.error(message);
      throw new IllegalArgumentException(message);
    }
    return clientProvider;
  }
}
