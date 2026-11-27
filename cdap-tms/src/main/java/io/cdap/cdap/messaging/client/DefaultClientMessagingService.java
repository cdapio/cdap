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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.messaging.spi.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client implementation of {@link MessagingService} that uses Messaging service endpoint.
 */
public class DefaultClientMessagingService extends AbstractClientMessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultClientMessagingService.class);

  @Inject
  public DefaultClientMessagingService(CConfiguration cConf,
      RemoteClientFactory remoteClientFactory) {
    super(remoteClientFactory.createRemoteClient(Constants.Service.MESSAGING_SERVICE,
            HTTP_REQUEST_CONFIG, "/v1/namespaces/"),
        cConf.getBoolean(Constants.MessagingSystem.HTTP_COMPRESS_PAYLOAD));
    LOG.info("DefaultClientMessagingService initialised.");
  }

  @VisibleForTesting
  public DefaultClientMessagingService(RemoteClientFactory remoteClientFactory,
      boolean compressPayload) {
    super(remoteClientFactory.createRemoteClient(Constants.Service.MESSAGING_SERVICE,
        HTTP_REQUEST_CONFIG, "/v1/namespaces/"), compressPayload);
  }
}
