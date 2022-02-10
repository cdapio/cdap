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
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.metadata.RemoteHealthCheckFetcher;
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.task.SupportBundleK8sHealthCheckTask;

/**
 * Support bundle pipeline info task factory to create pipeline info task which collect pipeline info and
 * generate them into files.
 */
public class SupportBundleK8sHealthCheckTaskFactory implements SupportBundleTaskFactory {

  private final CConfiguration cConfiguration;
  private final RemoteHealthCheckFetcher remoteHealthCheckFetcher;

  @Inject
  public SupportBundleK8sHealthCheckTaskFactory(CConfiguration cConfiguration,
                                                RemoteHealthCheckFetcher remoteHealthCheckFetcher) {
    this.cConfiguration = cConfiguration;
    this.remoteHealthCheckFetcher = remoteHealthCheckFetcher;
  }

  @Override
  public SupportBundleK8sHealthCheckTask create(SupportBundleTaskConfiguration taskConfiguration) {
    return new SupportBundleK8sHealthCheckTask(cConfiguration, taskConfiguration.getBasePath(),
                                               taskConfiguration.getNamespaces(), remoteHealthCheckFetcher);
  }
}
