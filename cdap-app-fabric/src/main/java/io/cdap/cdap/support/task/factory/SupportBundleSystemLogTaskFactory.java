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

package io.cdap.cdap.support.task.factory;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.logging.gateway.handlers.RemoteLogsFetcher;
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.handlers.RemoteMonitorServicesFetcher;
import io.cdap.cdap.support.task.SupportBundleSystemLogTask;

/**
 * Support Bundle system log task factory to create system log task to collect system logs for the data fusion instance.
 */
public class SupportBundleSystemLogTaskFactory implements SupportBundleTaskFactory {
  private final CConfiguration cConf;
  private final RemoteLogsFetcher remoteLogsFetcher;
  private final RemoteMonitorServicesFetcher remoteMonitorServicesFetcher;

  @Inject
  public SupportBundleSystemLogTaskFactory(CConfiguration cConf, RemoteLogsFetcher remoteLogsFetcher,
                                           RemoteMonitorServicesFetcher remoteMonitorServicesFetcher) {
    this.cConf = cConf;
    this.remoteLogsFetcher = remoteLogsFetcher;
    this.remoteMonitorServicesFetcher = remoteMonitorServicesFetcher;
  }

  @Override
  public SupportBundleSystemLogTask create(SupportBundleTaskConfiguration taskConfiguration) {
    return new SupportBundleSystemLogTask(taskConfiguration.getBasePath(), remoteLogsFetcher, cConf,
                                          remoteMonitorServicesFetcher);
  }
}
