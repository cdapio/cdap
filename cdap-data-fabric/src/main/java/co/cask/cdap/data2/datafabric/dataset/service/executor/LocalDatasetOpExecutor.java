/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.common.conf.CConfiguration;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes Dataset operations
 */
public class LocalDatasetOpExecutor extends RemoteDatasetOpExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(LocalDatasetOpExecutor.class);
  private final DatasetOpExecutorService executorServer;

  @Inject
  public LocalDatasetOpExecutor(CConfiguration cConf,
                                DiscoveryServiceClient discoveryClient,
                                DatasetOpExecutorService executorServer) {
    super(cConf, discoveryClient);
    this.executorServer = executorServer;
  }

  @Override
  protected void startUp() throws Exception {
    executorServer.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    executorServer.stopAndWait();
  }

  @Override
  protected Logger getUncaughtExceptionLogger() {
    return LOG;
  }
}
