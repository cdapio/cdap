/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.task;

import com.google.inject.Inject;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.logging.gateway.handlers.RemoteLogsFetcher;
import io.cdap.cdap.proto.SystemServiceMeta;
import io.cdap.cdap.support.handlers.RemoteMonitorServicesFetcher;
import io.cdap.cdap.support.lib.SupportBundleFileNames;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Collects support bundle system log from data fusion instance.
 */
public class SupportBundleSystemLogTask implements SupportBundleTask {

  private final File basePath;
  private final RemoteLogsFetcher remoteLogsFetcher;
  private final RemoteMonitorServicesFetcher remoteMonitorServicesFetcher;
  private final CConfiguration cConf;

  @Inject
  public SupportBundleSystemLogTask(File basePath, RemoteLogsFetcher remoteLogsFetcher,
                                    CConfiguration cConf, RemoteMonitorServicesFetcher remoteMonitorServicesFetcher) {
    this.basePath = basePath;
    this.remoteLogsFetcher = remoteLogsFetcher;
    this.remoteMonitorServicesFetcher = remoteMonitorServicesFetcher;
    this.cConf = cConf;
  }

  /**
   * Adds system logs into file
   */
  @Override
  public void collect() throws IOException, NotFoundException {
    File systemLogPath = new File(basePath, "system-log");
    DirUtils.mkdirs(systemLogPath);
    String componentId = "services";
    Iterable<SystemServiceMeta> serviceMetaList = remoteMonitorServicesFetcher.listSystemServices();
    for (SystemServiceMeta serviceMeta : serviceMetaList) {
      long currentTimeMillis = System.currentTimeMillis();
      long fromMillis =
        currentTimeMillis - TimeUnit.DAYS.toMillis(cConf.getInt(Constants.SupportBundle.SYSTEM_LOG_START_TIME));
      File file = new File(systemLogPath, serviceMeta.getName() + SupportBundleFileNames.SYSTEMLOG_SUFFIX_NAME);
      remoteLogsFetcher.writeSystemServiceLog(componentId, serviceMeta.getName(), fromMillis / 1000,
                                            currentTimeMillis / 1000, file);
    }
  }
}
