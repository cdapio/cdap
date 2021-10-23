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
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.gateway.handlers.RemoteProgramLogsFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Generates support bundle system log
 */
public class SupportBundleSystemLogTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleSystemLogTask.class);
  private final String systemLogPath;
  private final RemoteProgramLogsFetcher remoteProgramLogsFetcher;
  private final List<String> serviceList;

  @Inject
  public SupportBundleSystemLogTask(@Assisted String systemLogPath,
                                    RemoteProgramLogsFetcher remoteProgramLogsFetcher) {
    this.systemLogPath = systemLogPath;
    this.remoteProgramLogsFetcher = remoteProgramLogsFetcher;
    this.serviceList =
        Arrays.asList(
            Constants.Service.APP_FABRIC_HTTP,
            Constants.Service.DATASET_EXECUTOR,
            Constants.Service.EXPLORE_HTTP_USER_SERVICE,
            Constants.Service.LOGSAVER,
            Constants.Service.MESSAGING_SERVICE,
            Constants.Service.METADATA_SERVICE,
            Constants.Service.METRICS,
            Constants.Service.METRICS_PROCESSOR,
            Constants.Service.RUNTIME,
            Constants.Service.TRANSACTION,
            "pipeline");
  }

  /**
   * Adds system logs into file
   */
  public void initializeCollection() throws Exception {
    String componentId = "services";
    for (String serviceId : serviceList) {
      long currentTimeMillis = System.currentTimeMillis();
      long fromMillis = currentTimeMillis - TimeUnit.DAYS.toMillis(1);
      try (FileWriter file = new FileWriter(new File(systemLogPath, serviceId + "-system-log.txt"))) {
        String systemLog =
            remoteProgramLogsFetcher.getProgramSystemLog(
                componentId, serviceId, fromMillis / 1000, currentTimeMillis / 1000);
        file.write(systemLog);
      } catch (IOException e) {
        LOG.warn("Can not write system log file with service {} ", serviceId, e);
        throw new IOException("Can not write system log file ", e);
      }
    }
  }
}
