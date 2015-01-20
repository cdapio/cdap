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

package co.cask.cdap.data2.transaction.stream.inmemory;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.service.StreamCoordinator;
import co.cask.cdap.data2.transaction.queue.inmemory.InMemoryStreamAdmin;
import co.cask.cdap.data2.transaction.stream.AbstractStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A file based {@link co.cask.cdap.data2.transaction.stream.StreamAdmin} that maintains consumer state information
 * in memory.
 */
public final class InMemoryStreamFileAdmin extends AbstractStreamFileAdmin {

  @Inject
  InMemoryStreamFileAdmin(LocationFactory locationFactory, CConfiguration cConf,
                          StreamCoordinatorClient streamCoordinatorClient,
                          StreamConsumerStateStoreFactory stateStoreFactory,
                          NotificationFeedManager notificationFeedManager, InMemoryStreamAdmin oldStreamAdmin) {
    super(locationFactory, cConf, streamCoordinatorClient, stateStoreFactory, notificationFeedManager, oldStreamAdmin);
  }
}
