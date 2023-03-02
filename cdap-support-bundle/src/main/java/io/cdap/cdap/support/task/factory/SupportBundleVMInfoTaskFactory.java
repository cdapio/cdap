/*
 * Copyright © 2022 Cask Data, Inc.
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
import io.cdap.cdap.common.healthcheck.VMInformationFetcher;
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.metadata.RemoteMonitorServicesFetcher;
import io.cdap.cdap.support.task.SupportBundleTask;
import io.cdap.cdap.support.task.SupportBundleVMInfoTask;

/**
 * A {@link SupportBundleTaskFactory} for creating {@link SupportBundleVMInfoTask}.
 */
public class SupportBundleVMInfoTaskFactory implements SupportBundleTaskFactory {

  private final RemoteMonitorServicesFetcher servicesFetcher;
  private final VMInformationFetcher fetcher;

  @Inject
  SupportBundleVMInfoTaskFactory(RemoteMonitorServicesFetcher servicesFetcher,
      VMInformationFetcher fetcher) {
    this.servicesFetcher = servicesFetcher;
    this.fetcher = fetcher;
  }

  @Override
  public SupportBundleTask create(SupportBundleTaskConfiguration taskConfiguration) {
    return new SupportBundleVMInfoTask(servicesFetcher, fetcher, taskConfiguration.getBasePath());
  }
}
