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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.internal.app.runtime.schedule.store.DatasetBasedStreamSizeScheduleStore;
import co.cask.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import co.cask.cdap.internal.app.store.DefaultStore;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.protobuf.Service;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link Service} that will start the upgrade threads of Datasets and also provides the status of upgrade.
 */
public class AppVersionUpgradeService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(AppVersionUpgradeService.class);

  private final DatasetBasedStreamSizeScheduleStore streamSizeScheduleStore;
  private final DatasetBasedTimeScheduleStore timeScheduleStore;
  private final ExecutorService executorService;
  private final DefaultStore defaultStore;

  @Inject
  AppVersionUpgradeService(DatasetBasedStreamSizeScheduleStore streamSizeScheduleStore,
                           DatasetBasedTimeScheduleStore timeScheduleStore,
                           DefaultStore defaultStore) {
    this.streamSizeScheduleStore = streamSizeScheduleStore;
    this.timeScheduleStore = timeScheduleStore;
    this.defaultStore = defaultStore;
    this.executorService = Executors.newFixedThreadPool(
      3, Threads.createDaemonThreadFactory("app-version-upgrade-thread-%d"));
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting AppVersionUpgradeService.");
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          streamSizeScheduleStore.upgrade();
        } catch (Exception e) {
          LOG.debug("StreamSizeScheduleStore upgrade failed.", e);
        }
      }
    });
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          timeScheduleStore.upgrade();
        } catch (Exception e) {
          LOG.debug("TimeScheduleStore upgrade failed.", e);
        }
      }
    });
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          defaultStore.upgrade();
        } catch (Exception e) {
          LOG.debug("DefaultStore upgrade failed.", e);
        }
      }
    });
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down AppVersionUpgradeService.");
    executorService.shutdown();
  }

  public Map<String, Boolean> getUpgradeStatus() {
    Map<String, Boolean> upgradeStatus = new HashMap<>();
    upgradeStatus.put("streamSizeScheduleStore", streamSizeScheduleStore.isUpgradeComplete());
    upgradeStatus.put("timeScheduleStore", timeScheduleStore.isUpgradeComplete());
    upgradeStatus.put("defaultStore", defaultStore.isUpgradeComplete());
    return upgradeStatus;
  }
}
