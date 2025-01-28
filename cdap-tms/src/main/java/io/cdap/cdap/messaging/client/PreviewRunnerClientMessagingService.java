/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.messaging.client;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This helps Preview runner pods to communicate with MessagingService via Preview Manager.
 */
public class PreviewRunnerClientMessagingService extends AbstractClientMessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(
      PreviewRunnerClientMessagingService.class);

  @Inject
  public PreviewRunnerClientMessagingService(RemoteClientFactory remoteClientFactory) {
    // TODO (CDAP-21118) - enable gzip compression for preview http server.
    super(remoteClientFactory.createRemoteClient(Service.PREVIEW_HTTP, HTTP_REQUEST_CONFIG,
        "/v1/namespaces/"), false);
    LOG.info("PreviewRunnerClientMessagingService initialised");
  }
}
