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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.notifications.client.NotificationService;
import co.cask.cdap.notifications.inmemory.InMemoryNotificationService;
import co.cask.cdap.notifications.kafka.KafkaNotificationService;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

/**
 *
 */
public class NotificationServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new InMemoryNotificationServiceModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new InMemoryNotificationServiceModule();
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
//        bind(KafkaNotificationClient.class).in(Scopes.SINGLETON);
        bind(KafkaNotificationService.class).in(Scopes.SINGLETON);
      }

      @Provides
      @SuppressWarnings("unused")
      private NotificationService notificationPublisher(CConfiguration cConf, Injector injector) {
        // TODO use that constant once we have more core systems
        String coreSystem = cConf.get(Constants.Notification.TRANSPORT_SYSTEM, "kafka");
//        return injector.getInstance(KafkaNotificationClient.class);
        return injector.getInstance(KafkaNotificationService.class);
      }
    };
  }

  private static class InMemoryNotificationServiceModule extends AbstractModule {
    @Override
    protected void configure() {
      bind(NotificationService.class).to(InMemoryNotificationService.class).in(Scopes.SINGLETON);
    }
  }
}
