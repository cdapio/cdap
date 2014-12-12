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
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.notifications.NotificationPublisher;
import co.cask.cdap.notifications.NotificationSubscriber;
import co.cask.cdap.notifications.client.NotificationFeedClient;
import co.cask.cdap.notifications.kafka.KafkaNotificationPublisher;
import co.cask.cdap.notifications.kafka.KafkaNotificationSubscriber;
import co.cask.tephra.TransactionSystemClient;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.twill.kafka.client.KafkaClient;

/**
 *
 */
public class NotificationClientRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new NotificationClientModule();
  }

  @Override
  public Module getStandaloneModules() {
    return new NotificationClientModule();
  }

  @Override
  public Module getDistributedModules() {
    return new NotificationClientModule();
  }

  private static class NotificationClientModule extends AbstractModule {
    @Override
    protected void configure() {
    }

    @Provides
    @SuppressWarnings("unused")
    private NotificationPublisher notificationPublisher(CConfiguration cConf, NotificationFeedClient feedClient,
                                                        Injector injector) {
      // TODO use that constant once we have more core systems
      String coreSystem = cConf.get(Constants.Notification.CORE_SYSTEM, "kafka");
      return new KafkaNotificationPublisher(injector.getInstance(KafkaClient.class), feedClient);
    }

    @Provides
    @SuppressWarnings("unused")
    private NotificationSubscriber notificationSubscriber(CConfiguration cConf, NotificationFeedClient feedClient,
                                                          TransactionSystemClient txSystemClient,
                                                          Injector injector) {
      // TODO use that constant once we have more core systems
      String coreSystem = cConf.get(Constants.Notification.CORE_SYSTEM, "kafka");
      return new KafkaNotificationSubscriber(injector.getInstance(KafkaClient.class),
                                             injector.getInstance(DatasetFramework.class), feedClient, txSystemClient);
    }
  }
}
