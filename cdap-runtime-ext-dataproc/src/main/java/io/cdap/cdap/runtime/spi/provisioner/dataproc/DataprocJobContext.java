/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import io.cdap.cdap.runtime.spi.runtimejob.JobContext;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Map;

/**
 *
 */
public class DataprocJobContext implements JobContext {
  private final DiscoveryServiceClient discoveryServiceClient;
  private final DiscoveryService discoveryService;
  private final TwillRunnerService twillRunnerService;
  private final Map<String, String> configs;

  public DataprocJobContext(DiscoveryServiceClient discoveryServiceClient, DiscoveryService discoveryService,
                            TwillRunnerService twillRunnerService, Map<String, String> configs) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.discoveryService = discoveryService;
    this.twillRunnerService = twillRunnerService;
    this.configs = configs;
  }

  @Override
  public DiscoveryService getDiscoveryServiceSupplier() {
    return discoveryService;
  }

  @Override
  public DiscoveryServiceClient getDiscoveryServiceClientSupplier() {
    return discoveryServiceClient;
  }

  @Override
  public TwillRunnerService getTwillRunnerSupplier() {
    return twillRunnerService;
  }

  @Override
  public Map<String, String> getConfigProperties() {
    return configs;
  }
}
