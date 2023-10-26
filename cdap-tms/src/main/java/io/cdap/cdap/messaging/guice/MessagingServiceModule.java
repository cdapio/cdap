/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.messaging.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.MessagingSystem;
import io.cdap.cdap.messaging.client.DelegatingMessagingService;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.client.ClientMessagingService;

/**
 * The Guice module to bind messaging service based on {@link
 * io.cdap.cdap.common.conf.Constants.MessagingSystem#MESSAGING_SERVICE_NAME} if set in cConf.
 * Otherwise, binds to {@link ClientMessagingService}.
 */
public class MessagingServiceModule extends AbstractModule {

  private static final String DEFAULT_MESSAGING_SERVICE_NAME =
      ClientMessagingService.class.getSimpleName();
  private final String messagingService;

  public MessagingServiceModule(CConfiguration cConf) {
    messagingService =
        cConf
            .get(MessagingSystem.MESSAGING_SERVICE_NAME, DEFAULT_MESSAGING_SERVICE_NAME);
  }

  @Override
  protected void configure() {
    if (messagingService.equals(DEFAULT_MESSAGING_SERVICE_NAME)) {
      bind(MessagingService.class).to(ClientMessagingService.class).in(Scopes.SINGLETON);
    } else {
      bind(MessagingService.class).to(DelegatingMessagingService.class).in(Scopes.SINGLETON);
    }
  }
}
