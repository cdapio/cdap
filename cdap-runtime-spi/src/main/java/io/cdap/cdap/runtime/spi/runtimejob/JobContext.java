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


package io.cdap.cdap.runtime.spi.runtimejob;

import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Map;

/**
 *
 */
public interface JobContext {

  /**
   * Returns a {@link DiscoveryService} for service announcement purpose.
   */
  DiscoveryService getDiscoveryServiceSupplier();

  /**
   * Returns a {@link DiscoveryServiceClient} for service discovery purpose.
   */
  DiscoveryServiceClient getDiscoveryServiceClientSupplier();

  /**
   * Returns a {@link TwillRunnerService} for running programs.
   */
  TwillRunnerService getTwillRunnerSupplier();

  Map<String, String> getConfigProperties();
}
