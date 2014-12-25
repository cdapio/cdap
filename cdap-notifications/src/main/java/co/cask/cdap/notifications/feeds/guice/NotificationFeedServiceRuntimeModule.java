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

package co.cask.cdap.notifications.feeds.guice;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.feeds.service.InMemoryNotificationFeedStore;
import co.cask.cdap.notifications.feeds.service.MDSNotificationFeedStore;
import co.cask.cdap.notifications.feeds.service.NotificationFeedService;
import co.cask.cdap.notifications.feeds.service.NotificationFeedStore;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 * Guice modules for service side of the notification feed manager.
 */
public class NotificationFeedServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NotificationFeedStore.class).to(InMemoryNotificationFeedStore.class).in(Scopes.SINGLETON);
        bind(NotificationFeedManager.class).to(NotificationFeedService.class).in(Scopes.SINGLETON);
        expose(NotificationFeedManager.class);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NotificationFeedStore.class).to(MDSNotificationFeedStore.class).in(Scopes.SINGLETON);
        bind(NotificationFeedManager.class).to(NotificationFeedService.class).in(Scopes.SINGLETON);
        expose(NotificationFeedManager.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(NotificationFeedStore.class).to(MDSNotificationFeedStore.class).in(Scopes.SINGLETON);
        bind(NotificationFeedManager.class).to(NotificationFeedService.class).in(Scopes.SINGLETON);
        expose(NotificationFeedManager.class);
      }
    };
  }
}
