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
import com.google.inject.name.Names;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.ClientMessagingService;
import io.cdap.cdap.messaging.client.LocalClientMessagingService;

/**
 * The Guice module to provide binding for messaging system client.
 * This module should only be used in containers in distributed mode.
 */
public class MessagingClientModule extends AbstractModule {
  public static final String PROGRAM_MESSAGING = "programMessaging";
  private final boolean tetheringEnabled;

  public MessagingClientModule() {
    this(false);
  }

  public MessagingClientModule(boolean tetheringEnabled) {
    this.tetheringEnabled = tetheringEnabled;
  }

  @Override
  protected void configure() {
    bind(MessagingService.class).to(ClientMessagingService.class).in(Scopes.SINGLETON);
    if (tetheringEnabled) {
      // Publish program state messages to local TMS when running in tethered mode
      bind(MessagingService.class).annotatedWith(Names.named(PROGRAM_MESSAGING))
        .to(LocalClientMessagingService.class).in(Scopes.SINGLETON);
    } else {
      // Use the default messaging service for program state messages if we're not
      // running in tethered mode
      bind(MessagingService.class).annotatedWith(Names.named(PROGRAM_MESSAGING))
        .to(ClientMessagingService.class).in(Scopes.SINGLETON);
    }
  }
}
