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

package co.cask.cdap.messaging.guice;

import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.ClientMessagingService;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * The Guice module to provide binding for messaging system client.
 * This module should only be used in containers in distributed mode.
 */
public class MessagingClientModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(MessagingService.class).to(ClientMessagingService.class).in(Scopes.SINGLETON);
  }
}
