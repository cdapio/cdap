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

import com.google.common.collect.ArrayListMultimap;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.apache.tephra.zookeeper.TephraZKClientService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.util.concurrent.TimeUnit;

/**
 * Provides Guice binding to {@link ZKClient} and {@link ZKClientService}.
 */
public class ZKModule extends AbstractModule {

  @Override
  protected void configure() {
    /**
     * ZKClientService is provided by the provider method
     * {@link #provideZKClientService(org.apache.hadoop.conf.Configuration)}.
     */
    bind(ZKClient.class).to(ZKClientService.class);
  }

  @Provides
  @Singleton
  private ZKClientService provideZKClientService(Configuration conf) {
    String zkStr = conf.get(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM);
    if (zkStr == null) {
      // Default to HBase one.
      zkStr = conf.get(TxConstants.HBase.ZOOKEEPER_QUORUM);
    }

    int timeOut = conf.getInt(TxConstants.HBase.ZK_SESSION_TIMEOUT, TxConstants.HBase.DEFAULT_ZK_SESSION_TIMEOUT);
    ZKClientService zkClientService = new TephraZKClientService(zkStr, timeOut, null,
                                                                ArrayListMultimap.<String, byte[]>create());
    return ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(zkClientService, RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
        )
      )
    );
  }
}
