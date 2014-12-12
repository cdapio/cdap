/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.notifications.guice;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.cdap.notifications.client.NotificationFeedServiceClient;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * Guice modules for the client side of the Notification system.
 */
public class NotificationFeedClientRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new NotificationFeedClientModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new NotificationFeedClientModule();
  }

  @Override
  public Module getDistributedModules() {
    return new NotificationFeedClientModule();
  }

  private static class NotificationFeedClientModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(NotificationFeedClient.class).to(NotificationFeedServiceClient.class).in(Scopes.SINGLETON);
    }
  }
}
