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

package co.cask.cdap.app.guice;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.internal.app.runtime.notification.BasicNotificationFeedService;
import co.cask.cdap.notifications.service.NotificationFeedService;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * Guice modules for the service side of the Notification system.
 */
public class NotificationFeedServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new NotificationFeedServiceModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new NotificationFeedServiceModule();
  }

  @Override
  public Module getDistributedModules() {
    return new NotificationFeedServiceModule();
  }

  private static final class NotificationFeedServiceModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(NotificationFeedService.class).to(BasicNotificationFeedService.class).in(Scopes.SINGLETON);
    }

  }
}
