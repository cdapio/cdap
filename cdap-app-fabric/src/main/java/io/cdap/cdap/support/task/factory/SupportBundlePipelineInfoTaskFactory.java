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
import io.cdap.cdap.support.conf.SupportBundleConfiguration;
import io.cdap.cdap.support.task.SupportBundlePipelineInfoTask;

public class SupportBundlePipelineInfoTaskFactory implements SupportBundleTaskFactory {
  private final CConfiguration cConf;

  @Inject
  public SupportBundlePipelineInfoTaskFactory(CConfiguration cConf) {
    this.cConf = cConf;
  }

  public SupportBundlePipelineInfoTask create(
      SupportBundleConfiguration supportBundleConfiguration) {
    return new SupportBundlePipelineInfoTask(
        supportBundleConfiguration.getSupportBundleStatus(),
        supportBundleConfiguration.getNamespaceId(),
        supportBundleConfiguration.getBasePath(),
        supportBundleConfiguration.getApplicationClient(),
        supportBundleConfiguration.getProgramClient(),
        supportBundleConfiguration.getNumOfRunLogNeeded(),
        supportBundleConfiguration.getWorkflowName(),
        supportBundleConfiguration.getMetricsClient(),
        supportBundleConfiguration.getApplicationRecordList(),
        cConf);
  }
}
