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
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.task.SupportBundlePipelineInfoTask;

import java.util.List;

public class SupportBundlePipelineInfoTaskFactory implements SupportBundleTaskFactory {
  private final ProgramClient programClient;
  private final ApplicationClient applicationClient;
  private final MetricsClient metricsClient;

  @Inject
  public SupportBundlePipelineInfoTaskFactory(
      ProgramClient programClient,
      ApplicationClient applicationClient,
      MetricsClient metricsClient) {
    this.programClient = programClient;
    this.applicationClient = applicationClient;
    this.metricsClient = metricsClient;
  }

  public SupportBundlePipelineInfoTask create(
      SupportBundleStatus supportBundleStatus,
      String namespaceId,
      String basePath,
      String systemLogPath,
      int numOfRunNeeded,
      String workflowName,
      List<ApplicationRecord> applicationRecordList) {
    return new SupportBundlePipelineInfoTask(
        supportBundleStatus,
        namespaceId,
        basePath,
        applicationClient,
        programClient,
        numOfRunNeeded,
        workflowName,
        metricsClient,
        applicationRecordList);
  }
}
