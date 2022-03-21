/*
 * Copyright © 2022 Cask Data, Inc.
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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.messaging.MessagingService;

/**
 * The client implementation of {@link MessagingService} that *only* talks to the local messaging service even if
 * a runtime monitor url is configured.
 * This client is intended for internal higher level API implementation only.
 *
 * NOTE: This class shouldn't expose to end user (e.g. cdap-client module).
 */
public final class LocalClientMessagingService extends ClientMessagingService {
  @Inject
  LocalClientMessagingService(CConfiguration cConf, RemoteClientFactory remoteClientFactory) {
    super(remoteClientFactory.createRemoteClient(Constants.Service.MESSAGING_SERVICE,
                                                 HTTP_REQUEST_CONFIG,
                                                 NAMESPACE_PATH,
                                                 true),
          cConf.getBoolean(Constants.MessagingSystem.HTTP_COMPRESS_PAYLOAD));
  }
}
