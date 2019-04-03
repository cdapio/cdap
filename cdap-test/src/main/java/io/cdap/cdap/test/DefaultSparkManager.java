/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

package io.cdap.cdap.test;

import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * A default implementation of {@link SparkManager}.
 */
public class DefaultSparkManager extends AbstractProgramManager<SparkManager> implements SparkManager {

  private final DiscoveryServiceClient discoveryServiceClient;

  public DefaultSparkManager(ProgramId programId, ApplicationManager applicationManager,
                             DiscoveryServiceClient discoveryServiceClient) {
    super(programId, applicationManager);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public boolean isRunning() {
    // workaround until CDAP-7479 is fixed
    return super.isRunning() || !getHistory(ProgramRunStatus.RUNNING).isEmpty();
  }

  @Override
  public URL getServiceURL() {
    return getServiceURL(1, TimeUnit.SECONDS);
  }

  @Override
  public URL getServiceURL(long timeout, TimeUnit timeoutUnit) {
    return ServiceDiscoverable.createServiceBaseURL(
      new RandomEndpointStrategy(() -> discoveryServiceClient.discover(ServiceDiscoverable.getName(programId)))
        .pick(timeout, timeoutUnit), programId);
  }
}
