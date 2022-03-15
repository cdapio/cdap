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
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramLogsFetcher;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Collects support bundle system log from data fusion instance.
 */
public class SupportBundleSystemLogTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleSystemLogTask.class);
  private final File basePath;
  private final RemoteProgramLogsFetcher remoteProgramLogsFetcher;
  private final List<String> serviceList;

  @Inject
  public SupportBundleSystemLogTask(@Assisted File basePath, RemoteProgramLogsFetcher remoteProgramLogsFetcher) {
    this.basePath = basePath;
    this.remoteProgramLogsFetcher = remoteProgramLogsFetcher;
    this.serviceList = Arrays.asList(Constants.Service.APP_FABRIC_HTTP, Constants.Service.DATASET_EXECUTOR,
                                     Constants.Service.EXPLORE_HTTP_USER_SERVICE, Constants.Service.LOGSAVER,
                                     Constants.Service.MESSAGING_SERVICE, Constants.Service.METADATA_SERVICE,
                                     Constants.Service.METRICS, Constants.Service.METRICS_PROCESSOR,
                                     Constants.Service.RUNTIME, Constants.Service.TRANSACTION, "pipeline");
  }

  /**
   * Adds system logs into file
   */
  @Override
  public void collect() throws IOException, NotFoundException {
    File systemLogPath = new File(basePath, "system-log");
    DirUtils.mkdirs(systemLogPath);
    String componentId = "services";
    for (String serviceId : serviceList) {
      long currentTimeMillis = System.currentTimeMillis();
      long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
      try (FileWriter file = new FileWriter(
        new File(systemLogPath, serviceId + SupportBundleFileNames.SYSTEMLOG_SUFFIX_NAME))) {
        String systemLog = remoteProgramLogsFetcher.getProgramSystemLog(componentId, serviceId, fromMillis / 1000,
                                                                        currentTimeMillis / 1000);
        file.write(systemLog == null ? "" : systemLog);
      }
    }
  }
}
