/*
 * Copyright 2014 Cask, Inc.
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

import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Starts {@link DatasetOpExecutorService} in YARN.
 *
 * TODO: Currently the DatasetOpExecutorService (which this communicates with) is started by ReactorTwillApplication.
 * We want to start the DatasetOpExecutorService in this class startUp(), but it's not possible currently
 * since the service relies on MetricsClientRuntimeModules which is in watchdog module.
 */
public class YarnDatasetOpExecutor extends RemoteDatasetOpExecutor {

  @Inject
  public YarnDatasetOpExecutor(DiscoveryServiceClient discoveryClient) {
    super(discoveryClient);
  }

  @Override
  protected void startUp() throws Exception {
    // TODO: start {@link DatasetOpExecutorService} in YARN here
  }

  @Override
  protected void shutDown() throws Exception {

  }
}
