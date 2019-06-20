/*
 *
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

package io.cdap.cdap.schedule;

import com.google.inject.Inject;
import io.cdap.cdap.common.schedule.AbstractScheduleClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedule client to make requests to the App Fabric service which is discovered through {@link
 * DiscoveryServiceClient}.
 */
public class RemoteScheduleClient extends AbstractScheduleClient {

  @Inject
  public RemoteScheduleClient(DiscoveryServiceClient discoveryClient) {
    super(discoveryClient);
  }

}
